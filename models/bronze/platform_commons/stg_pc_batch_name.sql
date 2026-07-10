{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'batchName') }}
)

select
    id::bigint as batch_name_id,
    text::text as section,
    "languageCode"::text as language_code,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime
from raw
