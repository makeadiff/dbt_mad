{{ config(materialized='table') }}

-- CCI dashboard (E1): every session held by a mentoring circle since 1 June 2026, with who held it
-- and how long it ran (spec §3.10). Base for prod_cci_session_breakdown, prod_cci_volunteer_sessions
-- and prod_cci_child_sessions. Same "held" rule as prod_cci_session_dates (spec §3.4): a volunteer
-- present, or a substitute present (int_pc_volunteer_attendance). Not read by Dalgo.
--
-- Held by the original volunteer: a volunteer present. Held by a substitute: only a substitute
-- present (none logged so far).
-- Hours: the Platform Commons slot's start and end time. Taken from the slot linked to the circle
-- (stg_pc_worknode_slot); where the circle has no weekly slot linked, from the times on its
-- attendance records. 1 hour when neither has a usable time, or the end isn't after the start
-- (slot_times_missing says which). Times are text ("09:00").
--
-- Grain: one row per (worknode_slot_shift_id, session_date).

with attendance as (
    select
        a.section_slot_shift_id as worknode_slot_shift_id,
        a.scheduled_session_date::date as session_date,
        bool_or(a.attendance_status = 'PRESENT') as has_present,
        bool_or(a.substitute_volunteer_attendance_status = 'PRESENT') as has_substitute_present,
        max(case when a.center_slot_start_time ~ '^[0-9]{1,2}:[0-9]{2}' then a.center_slot_start_time end) as att_start,
        max(case when a.center_slot_end_time ~ '^[0-9]{1,2}:[0-9]{2}' then a.center_slot_end_time end) as att_end
    from {{ ref('int_pc_volunteer_attendance') }} a
    where a.scheduled_session_date::date >= date '2026-06-01'
    group by 1, 2
),

linked_slot as (
    select
        b.worknode_slot_shift_id,
        max(case when sl.start_time ~ '^[0-9]{1,2}:[0-9]{2}' then sl.start_time end) as slot_start,
        max(case when sl.end_time ~ '^[0-9]{1,2}:[0-9]{2}' then sl.end_time end) as slot_end
    from {{ ref('stg_pc_worknode_slot_shift_list_bridge') }} b
    join {{ ref('stg_pc_worknode_slot') }} sl
        on b.worknode_slot_id = sl.worknode_slot_id
    group by 1
),

timed as (
    select
        a.*,
        coalesce(ls.slot_start, a.att_start)::time as start_time,
        coalesce(ls.slot_end, a.att_end)::time as end_time
    from attendance a
    left join linked_slot ls
        on a.worknode_slot_shift_id = ls.worknode_slot_shift_id
)

select
    c.chapter_id,
    c.worknode_slot_shift_id,
    t.session_date,
    date_trunc('week', t.session_date)::date as week_start,
    t.has_present as held_by_original,
    (not t.has_present and coalesce(t.has_substitute_present, false)) as held_by_substitute,
    case
        when t.start_time is null or t.end_time is null or t.end_time <= t.start_time then 1.0
        else extract(epoch from (t.end_time - t.start_time)) / 3600.0
    end as slot_hours,
    (t.start_time is null or t.end_time is null or t.end_time <= t.start_time) as slot_times_missing
from timed t
join {{ ref('prod_cci_mentoring_circles') }} c
    on t.worknode_slot_shift_id = c.worknode_slot_shift_id
where t.has_present or coalesce(t.has_substitute_present, false)
