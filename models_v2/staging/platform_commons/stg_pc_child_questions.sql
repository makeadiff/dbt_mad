{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'childQuestions') }}
)

select
    id::bigint as child_question_id,
    question::text as question_text,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
