{{ config(materialized='table') }}

with raw_child as (
    select * from {{ source('bubble_staging', 'child') }}
)
select
    raw.child_id,
    raw.first_name,
    raw.last_name,
    raw.gender,
    raw.dob,
    raw.city,
    raw.date_of_enrollment,
    raw.mother_tounge,
    raw."Age" as age,
    raw.is_active,
    raw.removed,
    raw.class_id,
    raw.school_class_id,
    raw.school_id,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child raw
