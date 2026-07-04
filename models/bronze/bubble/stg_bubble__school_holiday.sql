{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'school_holiday') }}
)
select
    "school_holiday_id"::integer as school_holiday_id,
    "school_id" as school_id,
    "holiday_reason" as holiday_reason,
    "holiday_description" as holiday_description,
    "start_date"::date as start_date,
    "end_date"::date as end_date,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
