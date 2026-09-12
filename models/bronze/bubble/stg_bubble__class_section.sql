{{ config(materialized='table') }}

with source as (
    select * from {{ source('sessionops_raw', 'class_section') }}
)
select
    "class_section_id"::bigint as class_section_id,
    "section_name" as section_name,
    "section_code" as section_code,
    "section_display_name" as section_display_name,
    "removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "school_class_id"::bigint as school_class_id,
    "school_academic_year_id"::bigint as school_academic_year_id,
    "school_id"::bigint as school_id,
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
