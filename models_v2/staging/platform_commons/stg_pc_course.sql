{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'course') }}
)

select
    id::bigint as course_id,
    "courseCode"::text as course_code,
    "courseTypeCode"::text as course_type_code,
    "fieldId"::bigint as field_id,
    "tenant"::text as tenant_id,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
