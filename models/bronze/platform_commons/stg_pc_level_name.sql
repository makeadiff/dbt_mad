{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'levelName') }}
)

select
    id::bigint as level_name_id,
    text::text as class,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime
from raw
