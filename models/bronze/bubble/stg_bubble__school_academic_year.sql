{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'school_academic_year') }}
)
select
    "_id",
    "school_academic_year_id"::integer as school_academic_year_id,
    "school_id" as school_id,
    "academic_year_id" as academic_year_id,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
