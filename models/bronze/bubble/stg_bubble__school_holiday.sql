{{ config(materialized='table') }}

with source as (
    select * from {{ source('sessionops_raw', 'school_holiday') }}
)
select
    "school_holiday_id"::bigint as school_holiday_id,
    "school_id"::bigint as school_id,
    "holiday_reason" as holiday_reason,
    "holiday_description" as holiday_description,
    "remarks" as remarks,
    "start_date"::date as start_date,
    "end_date"::date as end_date,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "created_by_id"::bigint as created_by_id,
    "updated_by_id"::bigint as updated_by_id,
    "deleted_at"::timestamp as deleted_at,
    "created_at"::date as created_date,
    "updated_at"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
