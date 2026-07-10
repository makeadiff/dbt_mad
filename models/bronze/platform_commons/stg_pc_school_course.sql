{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'schoolCourse') }}
)

select
    id::bigint as school_course_id,
    "courseId"::bigint as course_id,
    "schoolId"::bigint as school_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
