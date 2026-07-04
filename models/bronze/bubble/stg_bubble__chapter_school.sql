{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'chapter_school') }}
)
select
    "chapter_school_id_number"::integer as chapter_school_id,
    "chapter_id_custom_chapter" as chapter_id,
    "academic_year_text" as academic_year,
    "co_id_user" as co_id,
    "school_id_custom_partner" as school_id,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
