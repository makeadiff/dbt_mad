{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'batchStudentAttendance') }}
)

select
    id::bigint as batch_student_attendance_id,
    "batchStudentId"::bigint as batch_student_id,
    "sclevelbatchId"::bigint as sc_level_batch_id,
    "baUID"::text as ba_uid,
    
    "attendanceStatus"::text as attendance_status,
    "attendanceContext"::text as attendance_context,
    "capturedByUser"::text as captured_by_user_id,
    "forSlotShiftId"::bigint as for_slot_shift_id,
    
    date::text as attendance_date,
    "subjectCode"::text as subject_code,
    
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
