{{ config(materialized='table') }}

-- CCI dashboard (E1): one row per child per CCI per period (spec §3.10; rules as DO §3.7 R4, R8).
-- A child's circles are those on their section (prod_cci_active_children, prod_cci_mentoring_circles);
-- sessions held are those circles' held dates in the period (prod_cci_circle_sessions), which needs
-- no slot day. Attended = the child marked present on a held date (int_pc_child_attendance), so
-- attended never exceeds held. Attendance % = attended / held. Status: Healthy >= 90%, At risk
-- 75-89%, Unhealthy < 75%. Hours of support = sessions attended x slot length (1 hour when a time is
-- missing). Only children with >= 1 held session in the period get a row. First name and child ID
-- only: no surname, nothing on health or family.
--
-- Dates: the child's attendance date can sit a day after the volunteer record's date for the same
-- session (the volunteer record's date comes through a day early, e.g. Friday for a Saturday slot), so a
-- child counts as attending a held session when their date is that day or the next. A circle has one
-- session a week, so this can't match two.
--
-- Grain: one row per (chapter_id, child_id, period_type, period_start).

with periods as (
    {{ session_periods() }}
),

child_circles as (
    select ac.chapter_id, ac.student_id as child_id, c.worknode_slot_shift_id
    from {{ ref('prod_cci_active_children') }} ac
    join {{ ref('prod_cci_mentoring_circles') }} c
        on ac.chapter_id = c.chapter_id
       and ac.sc_level_batch_id = c.sc_level_batch_id
),

child_present as (
    select distinct
        "ChildId" as child_id,
        "SectionSlotShiftId" as worknode_slot_shift_id,
        "ScheduledSessionDate"::date as session_date
    from {{ ref('int_pc_child_attendance') }}
    where "ChildAttendanceStatus" = 'PRESENT'
      and "ScheduledSessionDate"::date >= date '2026-10-01'
),

child_sessions as (
    select
        cc.chapter_id,
        cc.child_id,
        h.session_date,
        h.slot_hours,
        cp.child_id is not null as attended
    from child_circles cc
    join {{ ref('prod_cci_circle_sessions') }} h
        on cc.worknode_slot_shift_id = h.worknode_slot_shift_id
    left join child_present cp
        on cc.child_id = cp.child_id
       and h.worknode_slot_shift_id = cp.worknode_slot_shift_id
       and cp.session_date between h.session_date and h.session_date + 1
    where h.session_date >= date '2026-10-01'
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
),

first_names as (
    select s.student_id as child_id, split_part(trim(p.first_name), ' ', 1) as first_name
    from {{ ref('stg_pc_student') }} s
    join {{ ref('stg_pc_ie') }} i on s.ie_id = i.ie_id
    join {{ ref('stg_pc_person') }} pr on i.person_id = pr.person_id
    join {{ ref('stg_pc_person_profile') }} p on pr.person_profile_id = p.person_profile_id
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    pc.period_type,
    pc.period_label,
    pc.period_sort,
    pc.period_start,
    pc.period_end,
    pc.child_id,
    coalesce(fn.first_name, '') as child_first_name,
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
join {{ ref('prod_cci_chapter_summary') }} s
    on pc.chapter_id = s.chapter_id
left join first_names fn
    on pc.child_id = fn.child_id
