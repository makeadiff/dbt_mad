{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'university') }}
)

select
    id::bigint as university_id,
    "universityCode"::text as university_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
