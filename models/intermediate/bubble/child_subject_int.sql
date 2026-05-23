{{ config(materialized='table') }}

with raw_child_subject as (
    select * from {{ source('bubble_staging', 'child_subject') }}
)
select
    raw.child_subject_id,
    raw.academic_year,
    raw.child_id,
    raw.class_section_subject_id,
    raw.removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active
from raw_child_subject raw
