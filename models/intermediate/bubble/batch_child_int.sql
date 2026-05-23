{{ config(materialized='table') }}

with raw_batch_child as (
    select * from {{ source('bubble_staging', 'batch_child') }}
)
select
    raw.batch_child_id,
    raw.academic_year,
    raw.child_id,
    raw.school_id,
    raw.removed as removed_boolean,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active,
    raw.school_academic_year_id
from raw_batch_child raw
