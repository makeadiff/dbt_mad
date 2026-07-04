{{ config(materialized='view') }}

with raw_chapter as (
    select * from {{ source('bubble_raw', 'chapter') }}
)
select
    "chapter_id"::integer as chapter_id,
    "academic_year" as academic_year,
    "chapter_name" as chapter_name,
    "city" as city,
    "school_id" as school_id,
    "state" as state,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_chapter
