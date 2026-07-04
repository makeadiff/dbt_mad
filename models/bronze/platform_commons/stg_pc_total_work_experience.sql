{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'totalWorkExperience') }}
)

select
    id::bigint as total_work_experience_id,
    "value"::float as experience_value,
    "uomCode"::text as uom_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
