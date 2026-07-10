{{ config(materialized='table') }}

with raw_child as (
    select * from {{ source('bubble_raw', 'child') }}
)
select
    "_id",
    "child_id"::integer as child_id,
    "first_name" as first_name,
    "last_name" as last_name,
    "gender" as gender,
    "dob"::date as dob,
    "city" as city,
    "date_of_enrollment"::date as date_of_enrollment,
    "mother_tounge" as mother_tongue,
    "Age"::integer as age,
    "is_active"::boolean as is_active,
    "removed"::boolean as is_removed,
    "class_id" as class_id,
    "school_class_id" as school_class_id,
    "school_id" as school_id,
    "Created_By" as created_by,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_child
