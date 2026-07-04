{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'field_fieldName_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "fieldId"::bigint as field_id,
    "fieldNameId"::bigint as field_name_id
from raw
