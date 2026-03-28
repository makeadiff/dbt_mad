{{ config(materialized='view') }}

with raw_slot as (
    select * from {{ source('bubble_raw', 'slot') }}
)
select
    "_id",
    "slot_id_number"::integer as slot_id,
    "slot_name_text" as slot_name,
    "academic_year_text" as academic_year,
    "day_of_week_text" as day_of_week,
    "start_time_date"::timestamp as start_time,
    "end_time_date"::timestamp as end_time,
    "reccuring_boolean"::boolean as is_recurring,
    "school_id_custom_partner" as school_id,
    "removed_boolean"::boolean as is_removed,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_slot
