{{ config(materialized='view') }}

with raw_child as (
    select * from {{ source('bubble_raw', 'child') }}
)
select
    "_id",
    "child_id_number"::integer as child_id,
    "first_name_text" as first_name,
    "last_name_text" as last_name,
    "gender_text" as gender,
    "dob_date"::date as dob,
    "city_text" as city,
    "date_of_enrollment_date"::date as date_of_enrollment,
    "mother_tounge_text" as mother_tongue,
    "age_number"::integer as age,
    "is_active_boolean"::boolean as is_active,
    "removed_boolean"::boolean as is_removed,
    "class_id_custom_class" as class_id,
    "school_class_id_custom_school_class" as school_class_id,
    "school_id_custom_partner" as school_id,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_child
