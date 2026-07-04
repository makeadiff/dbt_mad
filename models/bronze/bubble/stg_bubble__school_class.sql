{{ config(materialized='view') }}

with raw_school_class as (
    select * from {{ source('bubble_raw', 'school_class') }}
)
select
    "_id",
    "school_class_id"::integer as school_class_id,
    "class_id" as class_id,
    "school_id" as school_id,
    "removed"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_school_class
