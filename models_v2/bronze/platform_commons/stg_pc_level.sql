{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'level') }}
)

select
    id::bigint as level_id,
    "levelCode"::text as level_code,
    "tenant"::text as tenant_id,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
