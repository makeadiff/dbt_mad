{{ config(materialized='table') }}

-- CCI dashboard (E1): planned sessions split into groups, one row per CCI per week (spec E1 §3.10
-- as amended by layout v4 §7 L1, copying prod_do_session_breakdown). Planned keeps cancelled dates
-- in; the display groups are held (original + substitute), cancelled, volunteer absent and no
-- volunteer assigned, and they add up to planned (test). Held by the original volunteer and held by
-- a substitute stay as separate columns for the second pie. Counts add up across weeks and within a
-- month; the % columns are % of planned, per row, never summed. Weeks and months as
-- prod_cci_weekly_sessions.
--
-- Circles with no weekly slot day have no planned dates, so they aren't sessions here; their count
-- is "Mentoring circles with no weekly slot" on prod_cci_chapters_display.
--
-- Grain: one row per (chapter_id, week_start); every CCI with children x every week to date.

with weeks as (
    select week_start, week_label, month_start, month_label
    from {{ ref('prod_cci_weekly_sessions') }}
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
    from {{ ref('prod_cci_planned_sessions') }}
    group by 1, 2
),

joined as (
    select
        s.chapter_id,
        s.chapter_name,
        s.city,
        s.district,
        s.do_name,
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
    from {{ ref('prod_cci_chapter_summary') }} s
    cross join weeks wk
    left join per_week pw
        on s.chapter_id = pw.chapter_id
       and wk.week_start = pw.week_start
    where s.has_children
)

select
    j.*,
    j.sessions_held_by_original + j.sessions_held_by_substitute as sessions_held,
    round(100.0 * j.sessions_held_by_original / nullif(j.sessions_planned, 0), 1) as pct_held_by_original,
    round(100.0 * j.sessions_held_by_substitute / nullif(j.sessions_planned, 0), 1) as pct_held_by_substitute,
    round(100.0 * (j.sessions_held_by_original + j.sessions_held_by_substitute) / nullif(j.sessions_planned, 0), 1) as pct_held,
    round(100.0 * j.sessions_cancelled / nullif(j.sessions_planned, 0), 1) as pct_cancelled
from joined j
