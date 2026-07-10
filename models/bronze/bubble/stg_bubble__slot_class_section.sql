{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'slot_class_section') }}
)
select
    "_id",
    "slot_class_section_id"::integer as slot_class_section_id,
    "slot_id" as slot_id,
    "class_section_id" as class_section_id,
    "class_section_subject_id" as class_section_subject_id,
    "academic_year" as academic_year,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
