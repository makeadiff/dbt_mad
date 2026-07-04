{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'school_academic_year') }}
)
select
    "school_academic_year_id"::integer as school_academic_year_id,
    "school_id" as school_id,
    "academic_year_id" as academic_year_id,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
