{{ config(materialized='table') }}

with source as (
    select * from {{ source('pc_raw', 'userRole') }}
)

select
    id::bigint as user_role_id,
    "roleCode"::text as role_code,
    "userId"::bigint as user_id,
    "userRoleMapStatusId"::bigint as user_role_map_status_id,
    "xModifiedTimestamp"::timestamp as modified_datetime,
    "xIsDeleted"::boolean as is_deleted,
    row_number() over (partition by "userId" order by id desc) as rn
from source
where "xIsDeleted" is false or "xIsDeleted" is null
