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
-- Programme (E1 spec §8 2026-11-13, point 2): each circle's section's programme; summing over
-- programmes gives the CCI's totals.
-- Grain: one row per (chapter_id, programme, week_start); every CCI with children x each programme
-- it has active children in x every week to date.

with weeks as (
    select week_start, week_label, month_start, month_label
    from {{ ref('prod_cci_weekly_sessions') }}
    group by 1, 2, 3, 4
),

circle_programme as (
    -- A circle sits on one section, so it has one programme: the section's (prod_cci_active_children).
    select c.worknode_slot_shift_id, sp.programme
    from {{ ref('prod_cci_mentoring_circles') }} c
    join (select distinct sc_level_batch_id, programme from {{ ref('prod_cci_active_children') }}) sp
        on c.sc_level_batch_id = sp.sc_level_batch_id
),

cci_programmes as (
    -- Every programme a CCI has active children in, so each CCI x programme gets every week.
    select distinct chapter_id, programme
    from {{ ref('prod_cci_active_children') }}
),

per_week as (
    select
        ps.chapter_id,
        cp.programme,
        ps.week_start,
        count(*) as sessions_planned,
        count(*) filter (where session_group = 'cancelled') as sessions_cancelled,
        count(*) filter (where session_group = 'held_by_original') as sessions_held_by_original,
        count(*) filter (where session_group = 'held_by_substitute') as sessions_held_by_substitute,
        count(*) filter (where session_group = 'volunteer_absent') as sessions_volunteer_absent,
        count(*) filter (where session_group = 'no_volunteer_assigned') as sessions_no_volunteer_assigned
    from {{ ref('prod_cci_planned_sessions') }} ps
    join circle_programme cp
        on ps.worknode_slot_shift_id = cp.worknode_slot_shift_id
    group by 1, 2, 3
),

joined as (
    select
        s.chapter_id,
        s.chapter_name,
        s.city,
        s.district,
        s.do_name,
        pg.programme,
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
    join cci_programmes pg
        on s.chapter_id = pg.chapter_id
    cross join weeks wk
    left join per_week pw
        on s.chapter_id = pw.chapter_id
       and pg.programme = pw.programme
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
