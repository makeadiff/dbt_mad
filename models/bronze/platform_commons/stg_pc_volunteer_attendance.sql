{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'communityMemberAttendance') }}
)

select
    id::bigint as attendance_id,
    "communityMemberId"::bigint as community_member_id,
    "forSlotShiftId"::bigint as for_slot_shift_id,
    "date"::timestamp as attendance_date,
    "attendanceStatus"::text as attendance_status,
    "zeroAttendanceStatus"::text as zero_attendance_status,
    "effortId"::bigint as effort_id,
    "subjectCode"::text as subject_code,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
