{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'slot_class_section_volunteer') }}
)
select
    "_id",
    "slot_class_section_volunteer_id"::integer as slot_class_section_volunteer_id,
    "slot_class_section_id" as slot_class_section_id,
    "volunteer_id" as volunteer_id,
    "academic_year" as academic_year,
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
