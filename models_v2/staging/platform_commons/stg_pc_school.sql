{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'school') }}
)

select
    id::bigint as school_id,
    "universityId"::bigint as university_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
