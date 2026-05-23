{{ config(materialized='table') }}

with raw_chapter as (
    select * from {{ source('bubble_staging', 'chapter') }}
)
select
    raw.chapter_id,
    raw.academic_year,
    raw.chapter_name,
    raw.city,
    raw.school_id,
    raw.state,
    raw.removed,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_chapter raw
