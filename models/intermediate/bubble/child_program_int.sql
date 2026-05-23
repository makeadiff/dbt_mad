{{ config(materialized='table') }}

with raw_child_program as (
    select * from {{ source('bubble_staging', 'child_program') }}
)
select
    raw.child_program_id,
    raw.academic_year,
    raw.child_id,
    raw.program_id,
    raw.removed as removed_boolean,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_program raw
