{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'co_chapter') }}
)
select
    "_id",
    "co_chapter_id"::integer as co_chapter_id,
    "chapter_id" as chapter_id,
    "academic_year" as academic_year,
    "co_id" as co_id,
    "start_date"::date as start_date,
    "end_date"::date as end_date,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
