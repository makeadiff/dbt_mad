{{ config(materialized='table') }}

with raw_class_section as (
    select * from {{ source('bubble_staging', 'class_section') }}
)
select
    raw.class_section_id,
    raw.academic_year,
    raw.section_name,
    raw.section_code,
    raw.removed,
    raw.is_active,
    raw.school_class_id,
    raw.school_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section raw
