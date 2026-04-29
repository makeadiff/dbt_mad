{{ config(materialized='view') }}

with raw_chapter as (
    select * from {{ source('bubble_raw', 'chapter') }}
)
select
    "chapter_id_number"::integer as chapter_id,
    "academic_year_text" as academic_year,
    "chapter_name_text" as chapter_name,
    "city_text" as city,
    "school_id_custom_partner" as school_id,
    "state_text" as state,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_chapter
