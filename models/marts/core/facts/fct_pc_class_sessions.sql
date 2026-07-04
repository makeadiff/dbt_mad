{{ config(materialized='table') }}

select
    session_key,
    class_key,
    "SessionId" as session_id,
    "ScheduledSessionDate" as session_date,
    "SlotMentorId" as mentor_id,
    "SubstituteId" as substitute_id,
    "SubstitutedUserId" as substituted_user_id,
    "RequestType" as substitution_type,
    "Reason" as substitution_reason,
    "CenterSlotName" as slot_name,
    "CenterSlotStartTime" as start_time,
    "CenterSlotEndTime" as end_time,
    "CenterSlotDayOfWeek" as day_of_week,
    "SessionActiveStatus" as is_active,
    "AttendanceTakenByUserId" as attendance_taken_by
from {{ ref('int_pc_class_ops_master') }}
