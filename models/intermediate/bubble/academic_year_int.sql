{{ config(materialized='table') }}

with raw_academic_year as (
    select * from {{ source('bubble_staging', 'academic_year') }}
)
select
    raw.academic_year_id,
    raw.label,
    raw.is_active,
    raw."Created_By" as created_by,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_academic_year raw
