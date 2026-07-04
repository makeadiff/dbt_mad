{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'optionName') }}
)

select
    id::bigint as option_name_id,
    text::text as option_label,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
