{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'slot_class_section_volunteer') }}
)
select
    "slot_class_section_volunteer_id"::integer as slot_class_section_volunteer_id,
    "slot_class_section_id" as slot_class_section_id,
    "volunteer_id" as volunteer_id,
    "academic_year" as academic_year,
    "removed"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
