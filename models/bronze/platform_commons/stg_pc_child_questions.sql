{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'question') }}
)

select
    id::bigint as child_question_id,
    text::text as question_text,
    true as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
