{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunity') }}
)

select
    id::bigint as opportunity_id,
    title::text as opportunity_name,
    "typeCode"::text as opportunity_type_code,
    "subTypeCode"::text as opportunity_sub_type_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
