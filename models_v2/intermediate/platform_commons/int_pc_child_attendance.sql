{{ config(
    materialized='table'
) }}

with attendance as (
    select * from {{ ref('stg_pc_batch_student_attendance') }}
)
-- Note: Further joins with user, school, batchStudent and feedbackResponses
-- can be added here. Feedback responses will be aggregated as JSONB using:
-- jsonb_object_agg(question_code, response_value) as dynamic_feedback

select
    a.batch_student_attendance_id,
    a.batch_student_id,
    a.sc_level_batch_id,
    
    a.attendance_status as "ChildAttendanceStatus",
    a.subject_code as "SubjectCode",
    a.attendance_date as "ScheduledSessionDate",
    a.captured_by_user_id as "AttendanceTakenByUserId",
    
    -- Placeholder for dynamic JSON feedback based on DBT best practices
    '{}'::jsonb as feedback_responses_json,
    
    current_timestamp as "_airbyte_extracted_at"

from attendance a
where a.is_deleted = false
