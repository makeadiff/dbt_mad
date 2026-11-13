{{ config(materialized='table') }}

-- DO dashboard (E2): one row per child per chapter per week (spec §3.9 W1), so child attendance
-- follows the Dalgo Week and Month filters. Same held and attended rules as prod_do_child_sessions
-- (held = the held circle-dates of the live sections the child is enrolled in; attended = marked
-- present on a held circle-date, so attended never exceeds held); only the time grain changes.
-- Weeks and months come from prod_do_weekly_sessions, the same source as
-- prod_do_session_breakdown, so Week / Week starting / Month / Month starting match
-- prod_do_session_breakdown_display exactly. Sessions count from 2026-10-01, so the weeks of a child
-- add up to their "Since 1 October" row in prod_do_child_sessions (test). Zero-held weeks get no
-- row. First name and child ID only (R8). No % or Status column: Dalgo computes them over the
-- weeks picked (W2, W3).
--
-- Grain: one row per (chapter_id, child_id, week_start).

with weeks as (
    select
        week_start,
        week_label,
        month_start,
        month_label,
        greatest(week_start, '2026-10-01'::date) as range_start,
        least(week_start + 6, (now() at time zone 'Asia/Kolkata')::date) as range_end
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1, 2, 3, 4
),

held as (
    select chapter_id, slot_id, class_section_id, session_date
    from {{ ref('prod_do_circle_sessions') }}
    where session_group in ('held_by_original', 'held_by_substitute')
),

enrolments as (
    select distinct ccs.child_id, ac.chapter_id, ccs.class_section_id
    from {{ ref('int_bubble__child_class_section') }} ccs
    join {{ ref('prod_do_active_children') }} ac
        on ccs.child_id = ac.child_id
    where not ccs.is_removed
),

child_attendance as (
    select distinct
        a.child_id,
        scs.slot_id,
        scs.class_section_id,
        a.date_of_slot::date as session_date
    from {{ ref('fct_e2_child_attendance_by_slot_date') }} a
    join {{ ref('int_bubble__slot_class_section') }} scs
        on a.slot_class_section_id = scs.slot_class_section_id
    where a.academic_year = '2026-2027'
      and upper(trim(a.attendance)) = 'TRUE'
),

child_sessions as (
    select
        e.child_id,
        e.chapter_id,
        h.session_date,
        ca.child_id is not null as attended
    from enrolments e
    join held h
        on e.chapter_id = h.chapter_id
       and e.class_section_id = h.class_section_id
    left join child_attendance ca
        on e.child_id = ca.child_id
       and h.slot_id = ca.slot_id
       and h.class_section_id = ca.class_section_id
       and h.session_date = ca.session_date
),

per_child as (
    select
        wk.week_start,
        wk.week_label,
        wk.month_start,
        wk.month_label,
        cs.chapter_id,
        cs.child_id,
        count(*) as sessions_held,
        count(*) filter (where cs.attended) as sessions_attended
    from weeks wk
    join child_sessions cs
        on cs.session_date between wk.range_start and wk.range_end
    group by 1, 2, 3, 4, 5, 6
)

select
    ch.engine,
    ch.chapter_id,
    ch.chapter_name,
    ch.city,
    ch.co_name,
    ch.cho_name,
    ch.district,
    ch.do_name,
    pc.week_start,
    pc.week_label,
    pc.month_start,
    pc.month_label,
    pc.child_id,
    coalesce(split_part(trim(c.first_name), ' ', 1), '') as child_first_name,
    pc.sessions_held,
    pc.sessions_attended
from per_child pc
join {{ ref('prod_do_chapter_summary') }} ch
    on pc.chapter_id = ch.chapter_id
left join {{ ref('int_bubble__children') }} c
    on pc.child_id = c.child_id
