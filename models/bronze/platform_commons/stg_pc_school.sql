{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'school') }}
)

select
    id::bigint as school_id,
    "universityId"::bigint as university_id,
    "isActive"::boolean as is_active,
    "schoolMediumCode"::text as school_medium_code,
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
