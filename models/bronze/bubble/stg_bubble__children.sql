{{ config(materialized='table') }}

with raw_child as (
    select * from {{ source('sessionops_raw', 'child') }}
)
select
    "child_id"::bigint as child_id,
    "first_name" as first_name,
    "last_name" as last_name,
    "gender" as gender,
    "date_of_birth"::date as dob,
    "city" as city,
    "date_of_enrollment"::date as date_of_enrollment,
    "mad_joining_date"::date as mad_joining_date,
    "mother_tongue" as mother_tongue,
    "age"::integer as age,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
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
from raw_child
