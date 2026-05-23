{{ config(materialized='table') }}

with raw_school_class as (
    select * from {{ source('bubble_staging', 'school_class') }}
)
select
    raw.school_class_id,
    raw.class_id,
    raw.school_id,
    raw.academic_year,
    raw.is_active,
    raw.removed,
    raw.school_academic_year_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_class raw
