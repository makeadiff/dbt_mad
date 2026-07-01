{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'worknodeHierarchy') }}
)

select
    id::bigint as worknode_hierarchy_id,
    "parentWorknodeId"::bigint as parent_worknode_id,
    "worknodeId"::bigint as worknode_id,
    depth::integer as depth,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
