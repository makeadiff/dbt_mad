{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'scLevelId') }}
)

select
    id::bigint as sc_level_id_table_id, -- Using a distinct name to avoid confusion with scLevelId column in scLevelBatch
    "levelId"::bigint as level_id,
    "schoolCourseId"::bigint as school_course_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
