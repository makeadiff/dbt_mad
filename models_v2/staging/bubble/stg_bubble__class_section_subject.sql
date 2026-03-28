{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'class_section_subject') }}
)
select
    "_id",
    "class_section_subject_id_number"::integer as class_section_subject_id,
    "academic_year_text" as academic_year,
    "class_section_id_custom_class_section" as class_section_id,
    "subject_id_custom_subject" as subject_id,
    "removed_boolean"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
