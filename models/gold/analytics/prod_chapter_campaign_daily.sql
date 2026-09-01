{{ config(materialized='table') }}

-- prod_chapter_campaign_daily: the frontline-mobilisation campaign board -- one row per active E2
-- chapter per campaign day, so the whole thing renders as a Dalgo Pivot Table with chapters down
-- the side, dates across the top, and recruited counts in the cells.
--
-- Window and goal are dbt vars so ops can re-run a different campaign without a code change:
--   dbt run -s prod_chapter_campaign_daily \
--     --vars '{campaign_start: "2026-09-01", campaign_end: "2026-09-05", campaign_goal: 10}'
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
-- Two rankings, both requested, both computed over the same window:
--   rank_first_to_goal -- primary. Who reached the goal first, ordered by the hire timestamp of
--                         the chapter's Nth qualifying volunteer. Null where the goal wasn't met.
--   rank_by_volume     -- secondary. Who recruited the most across the whole window.
-- They are genuinely different questions and can crown different chapters; ship both.
--
-- Chapter roster comes from prod_sric_dashboard_data rather than being rebuilt from dims. That is
-- a deliberate gold-on-gold reference: it makes the campaign board and the SRI dashboard agree by
-- construction on who counts as an active E2 chapter, and avoids a third hand-rolled copy of the
-- §6.9 filter (is_currently_active AND converted), which is precisely the duplication that
-- produced defect D2.

-- DEFAULT WINDOW: 6-10 July 2026, not the current week. Chosen 2026-08-28 because attribution
-- has had seven weeks to land on it, so the board renders with real numbers (19 recruited across
-- 8 chapters; Chennai's Singaram Pillay Girls crosses the goal on Thu 9 July) instead of the 340
-- zero cells the current week produces. This is a demonstration window for reviewing the layout,
-- NOT the live campaign -- when the real campaign runs, pass its dates in as vars.
{% set campaign_start = var('campaign_start', '2026-07-06') %}
{% set campaign_end   = var('campaign_end',   '2026-07-10') %}
{% set campaign_goal  = var('campaign_goal',  10) %}

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
goal_hit as (
    select chapter_id, hire_datetime as goal_reached_at
    from (
        select
            chapter_id,
            hire_datetime,
            row_number() over (partition by chapter_id order by hire_datetime, volunteer_id) as rn
        from window_intake
    ) ranked
    where rn = {{ campaign_goal }}
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
        (goal_reached_at is not null) as reached_goal,
        case when goal_reached_at is not null then
            dense_rank() over (order by goal_reached_at)
        end as rank_first_to_goal,
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
    -- Cumulative across the window, so a cell can be read as progress toward the goal rather than
    -- only as that day's activity.
    sum(d.recruited) over (
        partition by d.chapter_id order by d.date_key
        rows between unbounded preceding and current row
    ) as running_total,
    r.window_total,
    {{ campaign_goal }} as campaign_goal,
    r.reached_goal,
    r.goal_reached_at,
    r.rank_first_to_goal,
    r.rank_by_volume,
    u.window_hires_unattributed
from daily d
left join ranked r on d.chapter_id::text = r.chapter_id::text
cross join unattributed u
order by r.rank_first_to_goal nulls last, r.window_total desc, d.chapter, d.date_key
