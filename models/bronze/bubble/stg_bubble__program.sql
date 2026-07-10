{{ config(materialized='table') }}

with raw_program as (
    select * from {{ source('bubble_raw', 'program') }}
)
select
    "_id",
    "program_id"::integer as program_id,
    "program_name" as program_name,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_program
