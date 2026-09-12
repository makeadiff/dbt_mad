{{ config(materialized='table') }}

with raw_batch_child as (
    select * from {{ source('sessionops_raw', 'batch_child') }}
)
select
    "batch_child_id"::bigint as batch_child_id,
    "child_id"::bigint as child_id,
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
from raw_batch_child
