{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'userCreditPoint') }}
)

select
    id::bigint as user_credit_point_id,
    "userId"::bigint as user_id,
    "creditPoint"::text as credit_point,
    "lastPoint"::text as last_point,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
