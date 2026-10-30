{{ config(materialized='table') }}

-- DO dashboard (E2): one row per child per chapter per period (spec §3.7 R4 and R8, child table).
-- A child's circles are the live sections they're enrolled in (int_bubble__child_class_section);
-- sessions held are the held circle-dates of those circles in the period (volunteer or substitute
-- present, prod_do_circle_sessions), not planned. Attended = the child marked present on a held
-- circle-date (fct_e2_child_attendance_by_slot_date), so attended never exceeds held. Attendance %
-- = attended / held. Status: Healthy >= 90%, At risk 75-89%, Unhealthy < 75%. Hours of support =
-- sessions attended x the slot's length (1 hour when a time is missing, R5). Only children with >= 1
-- held session in the period get a row. First name and child ID only (R8): no surname, nothing on
-- health or family.
--
-- Grain: one row per (chapter_id, child_id, period_type, period_start).

with periods as (
    {{ session_periods() }}
),

held as (
    select chapter_id, slot_id, class_section_id, session_date, slot_hours
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
        h.slot_hours,
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
        pr.period_type,
        pr.period_label,
        pr.period_sort,
        pr.period_start,
        pr.period_end,
        cs.chapter_id,
        cs.child_id,
        count(*) as sessions_held,
        count(*) filter (where cs.attended) as sessions_attended,
        coalesce(sum(cs.slot_hours) filter (where cs.attended), 0) as hours_of_support
    from periods pr
    join child_sessions cs
        on cs.session_date between pr.period_start and pr.period_end
    group by 1, 2, 3, 4, 5, 6, 7
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
    pc.period_type,
    pc.period_label,
    pc.period_sort,
    pc.period_start,
    pc.period_end,
    pc.child_id,
    coalesce(split_part(trim(c.first_name), ' ', 1), '') as child_first_name,
    pc.sessions_held,
    pc.sessions_attended,
    round(100.0 * pc.sessions_attended / pc.sessions_held, 1) as attendance_pct,
    round(pc.hours_of_support::numeric, 1) as hours_of_support,
    case
        when pc.sessions_attended::numeric / pc.sessions_held >= 0.90 then 'Healthy'
        when pc.sessions_attended::numeric / pc.sessions_held >= 0.75 then 'At risk'
        else 'Unhealthy'
    end as status
from per_child pc
join {{ ref('prod_do_chapter_summary') }} ch
    on pc.chapter_id = ch.chapter_id
left join {{ ref('int_bubble__children') }} c
    on pc.child_id = c.child_id
