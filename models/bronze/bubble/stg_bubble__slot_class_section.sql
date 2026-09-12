{{ config(materialized='table') }}

with source as (
    select * from {{ source('sessionops_raw', 'slot_class_section') }}
)
select
    "slot_class_section_id"::bigint as slot_class_section_id,
    "slot_id"::bigint as slot_id,
    "class_section_id"::bigint as class_section_id,
    "class_section_subject_id"::bigint as class_section_subject_id,
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
