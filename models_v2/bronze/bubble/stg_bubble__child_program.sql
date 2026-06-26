{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'child_program') }}
)
select
    "child_program_id_number"::integer as child_program_id,
    "academic_year_text" as academic_year,
    "child_id_custom_child" as child_id,
    "program_id1_custom_program" as program_id,
    "removed_boolean"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
