{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'universityName') }}
)

select
    id::bigint as university_name_id,
    text::text as city_name,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
