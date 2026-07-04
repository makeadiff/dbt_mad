{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'iE') }}
)

select
    id::bigint as ie_id,
    "personId"::bigint as person_id,
    "loginName"::text as login_name,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
