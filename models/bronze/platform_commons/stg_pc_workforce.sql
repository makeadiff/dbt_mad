{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'workforce') }}
)

select
    id::bigint as workforce_id,
    "userId"::bigint as user_id,
    "worknodeId"::bigint as worknode_id,
    "roleId"::bigint as role_id,
    "isActive"::boolean as is_active,
    "xModifiedTimestamp"::timestamp as modified_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
