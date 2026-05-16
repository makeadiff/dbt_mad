{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'schoolName') }}
)

select
    id::bigint as school_name_id,
    text::text as center_name,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime
from raw
