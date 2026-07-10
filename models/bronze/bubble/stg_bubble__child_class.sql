{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'child_class') }}
)
select
    "_id",
    "child_class_id"::integer as child_class_id,
    "academic_year" as academic_year,
    "child_id" as child_id,
    "school_class_id" as school_class_id,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
