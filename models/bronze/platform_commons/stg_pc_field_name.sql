{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'fieldName') }}
)

select
    id::bigint as field_name_id,
    text::text as field_label,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
