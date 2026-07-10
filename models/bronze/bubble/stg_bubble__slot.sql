{{ config(materialized='table') }}

with raw_slot as (
    select * from {{ source('bubble_raw', 'slot') }}
)
select
    "_id",
    "slot_id"::integer as slot_id,
    "slot_name" as slot_name,
    "academic_year" as academic_year,
    "day_of_week" as day_of_week,
    "start_time"::timestamp as start_time,
    "end_time"::timestamp as end_time,
    "reccuring"::boolean as is_recurring,
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
from raw_slot
