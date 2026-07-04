{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'child_class_section') }}
)
select
    "child_class_section_id"::integer as child_class_section_id,
    "academic_year" as academic_year,
    "child_id" as child_id,
    "class_section_id" as class_section_id,
    "removed"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
