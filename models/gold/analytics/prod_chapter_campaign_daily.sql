{{ config(materialized='table') }}

-- prod_chapter_campaign_daily: the frontline-mobilisation campaign board -- one row per active E2
-- chapter per campaign day, so the whole thing renders as a Dalgo Pivot Table with chapters down
-- the side, dates across the top, and recruited counts in the cells.
--
-- Window and goal are dbt vars, set in dbt_project.yml (not the command line -- the nightly
-- 01:36 job only reads vars from there) so ops can re-run a different campaign without a code
-- change:
--   campaign_start: "2026-09-10"
--   campaign_end:   "2026-09-21"
--   campaign_goal:  null
--
-- HOW A VOLUNTEER LANDS IN A CELL (signed with Akshay 2026-08-28). The cell is the volunteer's
-- HIRE date. The chapter is their Bubble school mapping. The mapping's own date is deliberately
-- NOT a factor -- a volunteer counts from the day they were hired, but only becomes visible once
-- some chapter has claimed them. Cells therefore back-fill: a number that reads 0 today can read
-- 4 next month, for the same date, without anything being wrong.
--
-- Read this alongside fct_volunteer_chapter_intake's header: median hire -> mapping lag is 42 days
-- (p90 82). This board is a LAGGING record of a campaign week, not a live scoreboard, and
-- rankings should be treated as provisional until attribution settles. window_hires_unattributed
-- is carried on every row for exactly this purpose: while it is large, the ranking below it is
-- not yet final, and announcing a winner off this table would be announcing a guess.
--
-- CAMPAIGN_GOAL IS OPTIONAL (2026-09-13): the running 2026-09-10 to 2026-09-21 campaign has no
-- per-chapter goal, so campaign_goal is null and this board is a daily leaderboard by cumulative
-- recruits, not a race to a target. When campaign_goal is null, reached_goal, goal_reached_at and
-- rank_first_to_goal are all null (there is no goal to reach or rank against) -- do not invent a
-- placeholder number to make them non-null. rank_by_volume and running_total are the ranking that
-- matters here. If a future campaign sets a real campaign_goal var, rank_first_to_goal/
-- reached_goal/goal_reached_at populate again automatically, but the default sort below stays on
-- window_total (see ORDER BY note) regardless.
--
-- Two rankings, computed over the same window:
--   rank_first_to_goal -- who reached the goal first, ordered by the hire timestamp of the
--                         chapter's Nth qualifying volunteer. Null where there's no goal, or the
--                         goal wasn't met.
--   rank_by_volume     -- primary today. Who recruited the most across the whole window.
--
-- Chapter roster comes from prod_sric_dashboard_data rather than being rebuilt from dims. That is
-- a deliberate gold-on-gold reference: it makes the campaign board and the SRI dashboard agree by
-- construction on who counts as an active E2 chapter, and avoids a third hand-rolled copy of the
-- §6.9 filter (is_currently_active AND converted), which is precisely the duplication that
-- produced defect D2.

-- Fallback defaults below (6-10 July 2026, no goal) are a demonstration window for ad-hoc runs
-- outside the nightly job -- the live campaign's actual dates and goal come from the
-- dbt_project.yml vars above, which the nightly 01:36 job always reads.
{% set campaign_start = var('campaign_start', '2026-07-06') %}
{% set campaign_end   = var('campaign_end',   '2026-07-10') %}
{% set campaign_goal  = var('campaign_goal',  none) %}

with chapters as (
    select
        chapter_id,
        chapter,
        city,
        chapter_organiser
    from {{ ref('prod_sric_dashboard_data') }}
    where engine = 'E2'
      and chapter_status = true
),

days as (
    select date_key, day_label, day_short_name
    from {{ ref('dim_date') }}
    where date_key between date '{{ campaign_start }}' and date '{{ campaign_end }}'
),

-- Attributed hires falling inside the window. Unattributed ones are counted separately below
-- rather than dropped -- they are the pool that has not yet reached anybody's row.
window_intake as (
    select volunteer_id, chapter_id, hire_datetime, hire_date
    from {{ ref('fct_volunteer_chapter_intake') }}
    where is_chapter_attributed = true
      and hire_date between date '{{ campaign_start }}' and date '{{ campaign_end }}'
),

unattributed as (
    select count(*) as window_hires_unattributed
    from {{ ref('fct_volunteer_chapter_intake') }}
    where is_chapter_attributed = false
      and hire_date between date '{{ campaign_start }}' and date '{{ campaign_end }}'
),

-- Every chapter x every day, so a quiet day is a 0 and the pivot has no ragged columns.
grid as (
    select c.chapter_id, c.chapter, c.city, c.chapter_organiser, d.date_key, d.day_label, d.day_short_name
    from chapters c
    cross join days d
),

daily as (
    select g.*, count(wi.volunteer_id) as recruited
    from grid g
    left join window_intake wi
        on g.chapter_id::text = wi.chapter_id::text
       and g.date_key = wi.hire_date
    group by g.chapter_id, g.chapter, g.city, g.chapter_organiser, g.date_key, g.day_label, g.day_short_name
),

-- The moment a chapter's Nth volunteer was hired. Ordering by hire_datetime (not date) means the
-- race is settled to the second, so same-day finishes rank rather than tie.
-- When campaign_goal is null (no goal this campaign), this CTE deliberately returns no rows --
-- there is no Nth volunteer to look for -- so goal_reached_at is null for every chapter below.
goal_hit as (
    select chapter_id, hire_datetime as goal_reached_at
    from (
        select
            chapter_id,
            hire_datetime,
            row_number() over (partition by chapter_id order by hire_datetime, volunteer_id) as rn
        from window_intake
    ) ranked
    {% if campaign_goal is not none %}
    where rn = {{ campaign_goal }}
    {% else %}
    where false
    {% endif %}
),

chapter_totals as (
    select
        d.chapter_id,
        sum(d.recruited) as window_total,
        gh.goal_reached_at
    from daily d
    left join goal_hit gh on d.chapter_id::text = gh.chapter_id::text
    group by d.chapter_id, gh.goal_reached_at
),

ranked as (
    select
        chapter_id,
        window_total,
        goal_reached_at,
        {% if campaign_goal is not none %}
        (goal_reached_at is not null) as reached_goal,
        case when goal_reached_at is not null then
            dense_rank() over (order by goal_reached_at)
        end as rank_first_to_goal,
        {% else %}
        cast(null as boolean) as reached_goal,
        cast(null as bigint) as rank_first_to_goal,
        {% endif %}
        dense_rank() over (order by window_total desc) as rank_by_volume
    from chapter_totals
)

select
    d.chapter_id,
    d.chapter,
    d.city,
    d.chapter_organiser,
    d.date_key,
    d.day_label,
    d.day_short_name,
    d.recruited,
    -- Cumulative across the window, so a cell can be read as running progress rather than only as
    -- that day's activity -- the primary leaderboard metric while campaign_goal is null.
    sum(d.recruited) over (
        partition by d.chapter_id order by d.date_key
        rows between unbounded preceding and current row
    ) as running_total,
    r.window_total,
    {{ campaign_goal if campaign_goal is not none else 'cast(null as integer)' }} as campaign_goal,
    r.reached_goal,
    r.goal_reached_at,
    r.rank_first_to_goal,
    r.rank_by_volume,
    u.window_hires_unattributed
from daily d
left join ranked r on d.chapter_id::text = r.chapter_id::text
cross join unattributed u
-- Sorted by cumulative volume, not rank_first_to_goal -- there's no goal to race to by default
-- (see header). If a future campaign sets campaign_goal, rank_first_to_goal still populates as a
-- column, but the board's default sort stays on window_total.
order by r.window_total desc, d.chapter, d.date_key
