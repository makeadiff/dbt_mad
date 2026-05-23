{{ config(materialized='table') }}

with raw_subject as (
    select * from {{ source('bubble_staging', 'subject') }}
)
select
    raw.subject_id,
    raw.subject_name,
    raw.removed,
    raw.program_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_subject raw
