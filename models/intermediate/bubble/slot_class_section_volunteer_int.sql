{{ config(materialized='table') }}

with raw_slot_class_section_volunteer as (
    select * from {{ source('bubble_staging', 'slot_class_section_volunteer') }}
)
select
    raw.slot_class_section_volunteer_id,
    raw.slot_class_section_id,
    raw.volunteer_id,
    raw.academic_year,
    raw.is_active,
    raw.removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot_class_section_volunteer raw
