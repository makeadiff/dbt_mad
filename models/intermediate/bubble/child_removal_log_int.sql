{{ config(materialized='table') }}

with raw_child_removal_log as (
    select * from {{ source('bubble_staging', 'child_removal_log') }}
)
select
    raw.child_removal_log_id,
    raw.child_id,
    raw.co_id,
    raw.school_id,
    raw.other_details,
    raw.removal_reason,
    raw.removed,
    raw.is_active,
    raw.academic_year,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_removal_log raw
