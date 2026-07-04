{{ config(materialized='table') }}

select
    attendance_key,
    student_key,
    class_key,
    session_key,
    "ChildAttendanceStatus" as attendance_status,
    "ScheduledSessionDate" as session_date,
    "SubjectCode" as subject_code,
    feedback_json,
    did_actively_participate,
    did_understand_concepts,
    did_complete_assigned_task,
    additional_notes,
    reason_for_the_childs_absence
from {{ ref('int_pc_child_attendance') }}
