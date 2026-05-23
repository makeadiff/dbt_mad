{{ config(materialized='table') }}

with raw_child_class as (
    select * from {{ source('bubble_staging', 'child_class') }}
)
select
    raw.child_class_id,
    raw.academic_year,
    raw.child_id,
    raw.school_class_id,
    raw.removed as removed_boolean,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active
from raw_child_class raw
