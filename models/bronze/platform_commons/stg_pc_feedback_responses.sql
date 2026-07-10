{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'feedbackResponses') }}
)

select
    id::bigint as feedback_response_id,
    "questionId"::bigint as question_id,
    "subjectiveResponse"::text as response_value,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
