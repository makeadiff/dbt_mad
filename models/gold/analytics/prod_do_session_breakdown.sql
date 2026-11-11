{{ config(materialized='table') }}

-- DO dashboard (E2): planned sessions split into groups, one row per chapter per week (spec §3.7
-- R2 as amended by §3.8 ruling 1). Planned keeps cancelled dates in (unlike prod_do_weekly_sessions,
-- §3.3b); the display groups are held (original + substitute), cancelled, volunteer absent and no
-- volunteer assigned, and they add up to planned (test). Held by the original volunteer and held
-- by a substitute stay as separate columns for the second pie. Counts add up across weeks and
-- within a month; the % columns are per row and never summed. Weeks and months as
-- prod_do_weekly_sessions: Monday start, the first is the week of 28 Sep, the current one
-- "(still filling)", the month is the one holding the week's Thursday.
--
-- Circles with no slot have no day, so they aren't sessions here; their count is
-- "Mentoring circles with no weekly slot" on prod_do_chapters_display.
--
-- Grain: one row per (chapter_id, week_start); every chapter x every week to date.

with weeks as (
    select week_start, week_label, month_start, month_label
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1, 2, 3, 4
),

per_week as (
    select
        chapter_id,
        week_start,
        count(*) as sessions_planned,
        count(*) filter (where session_group = 'cancelled') as sessions_cancelled,
        count(*) filter (where session_group = 'held_by_original') as sessions_held_by_original,
        count(*) filter (where session_group = 'held_by_substitute') as sessions_held_by_substitute,
        count(*) filter (where session_group = 'volunteer_absent') as sessions_volunteer_absent,
        count(*) filter (where session_group = 'no_volunteer_assigned') as sessions_no_volunteer_assigned
    from {{ ref('prod_do_circle_sessions') }}
    group by 1, 2
),

chapters as (
    select engine, chapter_id, chapter_name, city, co_name, cho_name, district, do_name
    from {{ ref('prod_do_chapter_summary') }}
),

joined as (
    select
        ch.*,
        wk.week_start,
        wk.week_label,
        wk.month_start,
        wk.month_label,
        coalesce(pw.sessions_planned, 0) as sessions_planned,
        coalesce(pw.sessions_cancelled, 0) as sessions_cancelled,
        coalesce(pw.sessions_held_by_original, 0) as sessions_held_by_original,
        coalesce(pw.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
        coalesce(pw.sessions_volunteer_absent, 0) as sessions_volunteer_absent,
        coalesce(pw.sessions_no_volunteer_assigned, 0) as sessions_no_volunteer_assigned
    from weeks wk
    cross join chapters ch
    left join per_week pw
        on ch.chapter_id = pw.chapter_id
       and wk.week_start = pw.week_start
)

select
    j.*,
    j.sessions_held_by_original + j.sessions_held_by_substitute as sessions_held,
    round(100.0 * j.sessions_held_by_original / nullif(j.sessions_planned, 0), 1) as pct_held_by_original,
    round(100.0 * j.sessions_held_by_substitute / nullif(j.sessions_planned, 0), 1) as pct_held_by_substitute,
    round(100.0 * (j.sessions_held_by_original + j.sessions_held_by_substitute) / nullif(j.sessions_planned, 0), 1) as pct_held,
    round(100.0 * j.sessions_cancelled / nullif(j.sessions_planned, 0), 1) as pct_cancelled
from joined j
