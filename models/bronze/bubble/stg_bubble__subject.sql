{{ config(materialized='table') }}

with raw_subject as (
    select * from {{ source('bubble_raw', 'subject') }}
)
select
    "_id",
    "subject_id"::integer as subject_id,
    "subject_name" as subject_name,
    "removed"::boolean as is_removed,
    "program_id" as program_id,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_subject
