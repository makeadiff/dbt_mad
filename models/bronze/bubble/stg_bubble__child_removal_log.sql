{{ config(materialized='table') }}

with source as (
    select * from {{ source('sessionops_raw', 'child_removal_log') }}
)
select
    "child_removal_log_id"::bigint as child_removal_log_id,
    "child_id"::bigint as child_id,
    "co_id"::bigint as co_id,
    "other_details" as other_details,
    "removed_reason" as removal_reason,
    "removed_datetime"::timestamp as removed_datetime,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "school_id"::bigint as school_id,
    "deleted_at"::timestamp as deleted_at,
    "created_at"::date as created_date,
    "updated_at"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
