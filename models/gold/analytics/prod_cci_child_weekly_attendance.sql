{{ config(materialized='table') }}

-- CCI dashboard (E1): one row per child per CCI per week (DO spec §3.9 W1), so child attendance
-- follows the Dalgo Week and Month filters. Same held and attended rules as prod_cci_child_sessions
-- (held = the held dates of the circles on the child's section; attended = present on a held date
-- or the day after, since the volunteer record's date runs a day early); only the time grain
-- changes. Weeks and months come from prod_cci_weekly_sessions, the same source as
-- prod_cci_session_breakdown, so Week / Week starting / Month / Month starting match
-- prod_cci_session_breakdown_display exactly. Sessions count from 2026-10-01, so the weeks of a
-- child add up to their "Since 1 October" row in prod_cci_child_sessions (test). Zero-held weeks get
-- no row. First name and child ID only.
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
    from {{ ref('prod_cci_weekly_sessions') }}
    group by 1, 2, 3, 4
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
    pc.week_start,
    pc.week_label,
    pc.month_start,
    pc.month_label,
    pc.child_id,
    coalesce(fn.first_name, '') as child_first_name,
    pc.sessions_held,
    pc.sessions_attended
from per_child pc
join {{ ref('prod_cci_chapter_summary') }} s
    on pc.chapter_id = s.chapter_id
left join first_names fn
    on pc.child_id = fn.child_id
