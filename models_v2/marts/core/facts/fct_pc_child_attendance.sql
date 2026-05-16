{{ config(materialized='table') }}

select
    attendance_key,
    student_key,
    class_key,
    session_key,
    "ChildAttendanceStatus" as attendance_status,
    "ScheduledSessionDate" as session_date,
    "SubjectCode" as subject_code,
    feedback_json
from {{ ref('int_pc_child_attendance') }}
