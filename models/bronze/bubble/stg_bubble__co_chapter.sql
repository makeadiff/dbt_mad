{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'co_chapter') }}
)
select
    "co_chapter_id_number"::integer as co_chapter_id,
    "chapter_id_custom_chapter" as chapter_id,
    "academic_year_text" as academic_year,
    "co_id_user" as co_id,
    "start_date_date"::date as start_date,
    "end_date_date"::date as end_date,
    "is_active_boolean"::boolean as is_active,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
