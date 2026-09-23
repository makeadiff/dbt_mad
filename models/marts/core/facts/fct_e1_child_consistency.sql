{{ config(materialized='table') }}

-- fct_e1_child_consistency: per-child session consistency (sessions happened vs. attended) for one
-- academic year, for E1 (session-ops / platform_commons). E1 counterpart to
-- fct_e2_child_consistency.
-- Grain: one row per (student_id, school_id, academic_year) -- no chapter_id for E1, matching
-- fct_e1_school_coverage/fct_e1_session_summary/fct_e1_volunteer_consistency's existing choice.
-- Flow: int_pc_child_batch_enrollment (child -> batch -> school/academic_year, the row set) +
--       stg_pc_batch_attendance (sessions_happened per batch -- same "an attendance record exists
--       for this date" signal fct_e1_session_summary already uses, independent of any one child's
--       own attendance) + int_pc_child_attendance (this child's own PRESENT attendance, by batch)
--       -> fct_e1_child_consistency
-- Unlike fct_e1_volunteer_consistency, this has no planned-session estimation problem: a child's
-- "planned" side is simply how many sessions actually happened for their batch (sessions_happened),
-- same relationship fct_e2_child_consistency has to fct_e2_volunteer_attendance_by_slot_date.

with sessions_happened_per_batch as (
    select distinct
        sc_level_batch_id,
        attendance_date::date as session_date
    from {{ ref('stg_pc_batch_attendance') }}
),

sessions_happened_agg as (
    select
        sc_level_batch_id,
        count(distinct session_date) as sessions_happened
    from sessions_happened_per_batch
    group by sc_level_batch_id
),

child_attendance_agg as (
    select
        "ChildId" as student_id,
        "SchoolLevelBatchId" as sc_level_batch_id,
        count(distinct "ScheduledSessionDate") as attended_sessions
    from {{ ref('int_pc_child_attendance') }}
    where "ChildAttendanceStatus" = 'PRESENT'
      and "SchoolLevelBatchId" is not null
    group by "ChildId", "SchoolLevelBatchId"
),

joined as (
    select
        cbe.student_id,
        cbe.student_name,
        cbe.school_id,
        cbe.academic_year,
        coalesce(sh.sessions_happened, 0) as sessions_happened,
        coalesce(ca.attended_sessions, 0) as attended_sessions
    from {{ ref('int_pc_child_batch_enrollment') }} cbe
    left join sessions_happened_agg sh
        on cbe.batch_id = sh.sc_level_batch_id
    left join child_attendance_agg ca
        on cbe.student_id = ca.student_id
        and cbe.batch_id = ca.sc_level_batch_id
),

child_aggregated as (
    select
        student_id,
        student_name,
        school_id,
        academic_year,
        sum(sessions_happened) as sessions_happened,
        sum(attended_sessions) as attended_sessions
    from joined
    group by student_id, student_name, school_id, academic_year
)

select
    student_id,
    student_name,
    school_id,
    academic_year,
    sessions_happened,
    attended_sessions,
    -- TODO: update multiplier when actual session duration per slot is available.
    attended_sessions * 2 as hours_of_support,
    case
        when sessions_happened = 0 then null
        else round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1)
    end as attendance_pct,
    case
        when sessions_happened = 0 then 'No Sessions Yet'
        when round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1) >= 90 then 'Healthy'
        when round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1) >= 75 then 'At Risk'
        else 'Unhealthy'
    end as consistency_status
from child_aggregated
order by school_id, consistency_status
