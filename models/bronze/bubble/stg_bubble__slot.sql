{{ config(materialized='table') }}

with raw_slot as (
    select * from {{ source('sessionops_raw', 'slot') }}
)
select
    "slot_id"::bigint as slot_id,
    "slot_name" as slot_name,
    "day_of_week" as day_of_week,
    "start_time"::time as start_time,
    "end_time"::time as end_time,
    "recurring"::boolean as is_recurring,
    "school_id"::bigint as school_id,
    "school_academic_year_id"::bigint as school_academic_year_id,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "created_by_id"::bigint as created_by_id,
    "updated_by_id"::bigint as updated_by_id,
    "deleted_at"::timestamp as deleted_at,
    "created_at"::date as created_date,
    "updated_at"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_slot
