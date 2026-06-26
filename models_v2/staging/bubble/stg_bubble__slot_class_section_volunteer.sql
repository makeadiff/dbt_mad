{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'slot_class_section_volunteer') }}
)
select
    "slot_class_section_volunteer_id_number"::integer as slot_class_section_volunteer_id,
    "slot_class_section_id_custom_volunteer_class_section" as slot_class_section_id,
    "volunteer_id_user" as volunteer_id,
    "academic_year_text" as academic_year,
    "removed_boolean"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
