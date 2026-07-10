{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'course_courseName_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "courseId"::bigint as course_id,
    "courseNameId"::bigint as course_name_id
from raw
