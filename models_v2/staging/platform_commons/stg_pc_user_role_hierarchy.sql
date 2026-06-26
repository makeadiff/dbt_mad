{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'userRoleHierarchy') }}
)

select
    id::bigint as user_role_hierarchy_id,
    "userRoleId"::bigint as user_role_id,
    "parentUserRoleId"::bigint as parent_user_role_id,
    "function"::text as function_type,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
