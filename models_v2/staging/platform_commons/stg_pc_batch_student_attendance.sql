{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'batchStudentAttendance') }}
)

select
    id as batch_student_attendance_id,
    "batchStudentId" as batch_student_id,
    "sclevelbatchId" as sc_level_batch_id,
    
    "attendanceStatus" as attendance_status,
    "attendanceContext" as attendance_context,
    "capturedByUser" as captured_by_user_id,
    
    date as attendance_date,
    "subjectCode" as subject_code,
    
    "createdDateTime" as created_datetime,
    "xIsDeleted" as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
