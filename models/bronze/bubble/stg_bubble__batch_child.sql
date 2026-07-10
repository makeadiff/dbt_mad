{{ config(materialized='table') }}

with raw_batch_child as (
    select * from {{ source('bubble_raw', 'batch_child') }}
)
select
    "_id",
    "batch_child_id"::integer as batch_child_id,
    "academic_year" as academic_year,
    "child_id" as child_id,
    "school_id" as school_id,
    "school_academic_year_id" as school_academic_year_id,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_batch_child
