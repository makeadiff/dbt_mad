{{ config(materialized='table') }}

-- DO dashboard (E2): planned sessions split into five groups, one row per chapter per week (spec
-- §3.7 R2). Planned keeps cancelled dates in (unlike prod_do_weekly_sessions, §3.3b); the groups
-- are, in order: cancelled, held by the original volunteer, held by a substitute, volunteer absent,
-- no attendance logged. They add up to planned (test). Counts add up across weeks; the % columns
-- are per row and never summed. Weeks as prod_do_weekly_sessions: Monday start, the first is the
-- week of 28 Sep, the current one "(still filling)".
--
-- Circles with no slot have no day, so they aren't sessions here; their count is
-- "Mentoring circles with no volunteer" on prod_do_chapters_display.
--
-- Grain: one row per (chapter_id, week_start); every chapter x every week to date.

with weeks as (
    select week_start, week_label from {{ ref('prod_do_weekly_sessions') }} group by 1, 2
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
        count(*) filter (where session_group = 'no_attendance') as sessions_no_attendance_logged
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
        coalesce(pw.sessions_planned, 0) as sessions_planned,
        coalesce(pw.sessions_cancelled, 0) as sessions_cancelled,
        coalesce(pw.sessions_held_by_original, 0) as sessions_held_by_original,
        coalesce(pw.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
        coalesce(pw.sessions_volunteer_absent, 0) as sessions_volunteer_absent,
        coalesce(pw.sessions_no_attendance_logged, 0) as sessions_no_attendance_logged
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
