{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'class_section') }}
)
select
    "_id",
    "class_section_id"::integer as class_section_id,
    "academic_year" as academic_year,
    "section_name" as section_name,
    "section_code" as section_code,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "school_class_id" as school_class_id,
    "school_id" as school_id,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
