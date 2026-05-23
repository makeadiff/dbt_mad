{{ config(materialized='table') }}

with raw_class_section_subject as (
    select * from {{ source('bubble_staging', 'class_section_subject') }}
)
select
    raw.class_section_subject_id,
    raw.academic_year,
    raw.class_section_id,
    raw.subject_id,
    raw.removed,
    raw.is_active,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section_subject raw
