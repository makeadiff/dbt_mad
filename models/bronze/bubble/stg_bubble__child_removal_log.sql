{{ config(materialized='table') }}

with source as (
    select * from {{ source('bubble_raw', 'child_removal_log') }}
)
select
    "_id",
    "child_removal_log_id"::integer as child_removal_log_id,
    "academic_year" as academic_year,
    "child_id" as child_id,
    "co_id" as co_id,
    "other_details" as other_details,
    "removal_reason" as removal_reason,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "school_id" as school_id,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
