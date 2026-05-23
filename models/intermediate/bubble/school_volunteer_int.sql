{{ config(materialized='table') }}

with raw_school_volunteer as (
    select * from {{ source('bubble_staging', 'school_volunteer') }}
)
select
    raw.school_volunteer_id,
    raw.academic_year,
    raw.school_id,
    raw.volunteer_id,
    raw.is_active,
    raw.removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_volunteer raw
