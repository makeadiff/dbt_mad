{{ config(materialized='view') }}

with raw_subject as (
    select * from {{ source('bubble_raw', 'subject') }}
)
select
    "_id",
    "subject_id_number"::integer as subject_id,
    "subject_name_text" as subject_name,
    "removed_boolean"::boolean as is_removed,
    "program_id_custom_program" as program_id,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_subject
