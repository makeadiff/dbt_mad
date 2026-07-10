{{ config(materialized='table') }}

with raw_academic_year as (
    select * from {{ source('bubble_raw', 'academic_year') }}
)
select
    "_id",
    "academic_year_id"::integer as academic_year_id,
    "label" as label,
    "is_active"::boolean as is_active,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_academic_year
