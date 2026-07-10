{{ config(materialized='table') }}

with raw_class as (
    select * from {{ source('bubble_raw', 'class') }}
)
select
    "_id",
    "class_id"::integer as class_id,
    "class_name" as class_name,
    "class_code"::integer as class_code,
    "program_id"::integer as program_id,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_class
