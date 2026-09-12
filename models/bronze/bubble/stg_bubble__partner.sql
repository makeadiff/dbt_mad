{{ config(materialized='table') }}

with source as (
    select * from {{ source('sessionops_raw', 'partner') }}
)
select
    "partner_id"::bigint as partner_id,
    "city" as city,
    null::bigint as created_by, -- sessionops's Partner model has no created_by/updated_by at all (SoftDeleteBaseModel with only deleted_by)
    "co_id"::bigint as co_id,
    "state" as state,
    "created_at"::date as created_date,
    "co_name" as co_name,
    "mou_url" as mou_url,
    "updated_at"::date as modified_date,
    "poc_name" as poc_name,
    "city_id"::bigint as city_id,
    "pincode"::integer as pincode,
    "poc_email" as poc_email,
    "state_id"::bigint as state_id,
    "lead_source" as lead_source,
    "school_type" as school_type,
    "classes" as classes_list,
    "mou_end_date"::date as mou_end_date,
    "partner_name" as partner_name,
    "mou_sign_date"::date as mou_sign_date,
    "partner_id"::bigint as partner_id1,
    "poc_contact" as poc_contact,
    "address_line_1" as address_line_1,
    "address_line_2" as address_line_2,
    "mou_start_date"::date as mou_start_date,
    "poc_designation" as poc_designation,
    "total_child_count"::integer as total_child_count,
    "date_of_first_contact"::timestamp as date_of_first_contact,
    null::boolean as low_income_resource, -- dropped in sessionops's Partner model, no equivalent field
    "confirmed_child_count"::integer as confirmed_child_count,
    "partner_affiliation_type" as partner_affiliation_type,
    "converted"::boolean as converted,
    "latest_conversion_stage" as latest_conversion_stage,
    "crm_partner_removed"::boolean as is_removed,
    "is_active"::boolean as is_active,
    "synced_at"::timestamp as synced_at,
    "deleted_by_id"::bigint as deleted_by_id,
    "deleted_at"::timestamp as deleted_at,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from source
