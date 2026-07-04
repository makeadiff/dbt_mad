{{ config(materialized='view') }}

with raw_batch_child as (
    select * from {{ source('bubble_raw', 'batch_child') }}
)
select
    "batch_child_id"::integer as batch_child_id,
    "academic_year" as academic_year,
    "child_id" as child_id,
    "school_id" as school_id,
    "removed"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_batch_child
