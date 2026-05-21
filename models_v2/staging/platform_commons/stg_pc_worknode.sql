{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'worknode') }}
)

select
    id::bigint as worknode_id,
    name::text as worknode_name,
    type::text as worknode_type,
    code::text as worknode_code,
    "linkedSystemId"::bigint as linked_system_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
