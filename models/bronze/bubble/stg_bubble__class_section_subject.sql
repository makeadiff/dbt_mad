{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'class_section_subject') }}
)
select
    "_id",
    "class_section_subject_id"::integer as class_section_subject_id,
    "academic_year" as academic_year,
    "class_section_id" as class_section_id,
    "subject_id" as subject_id,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
