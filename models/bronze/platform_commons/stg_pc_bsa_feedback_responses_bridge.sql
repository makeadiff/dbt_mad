{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'bsaFeedbackResponses_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "batchStudentAttendanceId"::bigint as batch_student_attendance_id,
    "feedbackResponsesId"::bigint as feedback_response_id
from raw
