{{ config(materialized='table') }}

with classes as (
    select distinct
        class_key,
        "BatchId" as batch_id,
        "ClassName" as class_name,
        "CourseName" as course_name,
        "Section" as section_name,
        "SectionId" as section_id,
        "Stream" as stream,
        "batchStatus" as batch_status,
        "CenterName" as center_name,
        "CityName" as city_name,
        "SchoolId" as school_id
    from {{ ref('int_pc_class_ops_master') }}
)

select * from classes
