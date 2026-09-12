{{ config(materialized='table') }}

with raw_subject as (
    select * from {{ source('sessionops_raw', 'subject') }}
)
select
    "subject_id"::bigint as subject_id,
    "subject_name" as subject_name,
    false as is_removed, -- sessionops subject has no removed/is_active/deleted_at at all (universal catalog, never deactivated)
    "program_id"::bigint as program_id,
    "created_by_id"::bigint as created_by_id,
    "updated_by_id"::bigint as updated_by_id,
    "created_at"::date as created_date,
    "updated_at"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_subject
