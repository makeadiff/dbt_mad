{{ config(materialized='table') }}

with raw_chapter as (
    select * from {{ source('bubble_raw', 'chapter') }}
)
select
    "_id",
    "chapter_id"::integer as chapter_id,
    "academic_year" as academic_year,
    "chapter_name" as chapter_name,
    "city" as city,
    "school_id" as school_id,
    "state" as state,
    "removed"::boolean as is_removed,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_chapter
