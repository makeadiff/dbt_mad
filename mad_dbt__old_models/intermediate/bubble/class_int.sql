{{ config(materialized='table') }}

with raw_class as (
    select * from {{ source('bubble_staging', 'class') }}
)
select
    raw.class_id::integer               as class_id,
    raw.class_name,
    raw.class_code::integer             as class_code,
    raw.program_id::integer             as program_id,
    raw."Created_By"                    as created_by,
    raw."Created_Date"                  as created_date,
    raw."Modified_Date"                 as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class raw
