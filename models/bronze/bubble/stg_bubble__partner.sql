{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'partner') }}
)
select
    "_id" as partner_id,
    "city" as city,
    "Created_By" as created_by,
    "co_id" as co_id,
    "state" as state,
    "Created_Date"::date as created_date,
    "co_name" as co_name,
    "mou_url" as mou_url,
    "Modified_Date"::date as modified_date,
    "poc_name" as poc_name,
    "city_id"::integer as city_id,
    "pincode"::integer as pincode,
    "poc_email" as poc_email,
    "state_id"::integer as state_id,
    "lead_source" as lead_source,
    "school_type" as school_type,
    "classes" as classes_list,
    "mou_end_date"::date as mou_end_date,
    "partner_name" as partner_name,
    "mou_sign_date"::date as mou_sign_date,
    "partner_id"::integer as partner_id1,
    "poc_contact" as poc_contact,
    "address_line_1" as address_line_1,
    "address_line_2" as address_line_2,
    "mou_start_date"::date as mou_start_date,
    "poc_designation" as poc_designation,
    "total_child_count"::integer as total_child_count,
    "date_of_first_contact"::date as date_of_first_contact,
    "low_income_resource"::boolean as low_income_resource,
    "confirmed_child_count"::integer as confirmed_child_count,
    "partner_affiliation_type" as partner_affiliation_type,
    "partner_removed"::boolean as is_removed,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
