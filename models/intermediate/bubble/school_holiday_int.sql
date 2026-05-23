{{ config(materialized='table') }}

with raw_school_holiday as (
    select * from {{ source('bubble_staging', 'school_holiday') }}
)
select
    raw.school_holiday_id,
    raw.school_id,
    raw.start_date,
    raw.end_date,
    raw.holiday_reason,
    raw.holiday_description,
    raw.is_active,
    raw.removed,
    raw."Created_By" as created_by,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_holiday raw
