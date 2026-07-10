{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'field') }}
)

select
    id::bigint as field_id,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
