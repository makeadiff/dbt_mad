{{ config(materialized='table') }}

with raw_partner as (
    select * from {{ source('bubble_staging', 'partner') }}
)
select
    raw."_id"                               as partner_id,   -- Bubble _id (alphanumeric text)
    raw.partner_id::integer                 as partner_id1,  -- numeric partner ID
    raw.partner_name,
    raw.city,
    raw.state,
    raw.city_id,
    raw.state_id,
    raw.pincode,
    raw.address_line_1,
    raw.address_line_2,
    raw.school_type,
    raw.classes                             as classes_list,
    raw.co_name,
    raw.co_id                               as co_id_user,
    raw.poc_name,
    raw.poc_email,
    raw.poc_contact,
    raw.poc_designation,
    raw.lead_source,
    raw.mou_url,
    raw.mou_sign_date,
    raw.mou_start_date,
    raw.mou_end_date,
    raw.total_child_count,
    raw.confirmed_child_count,
    raw.low_income_resource,
    raw.date_of_first_contact,
    raw.partner_affiliation_type,
    raw.converted,
    raw.latest_conversion_stage,
    raw.partner_removed,
    raw.removed,
    raw."Created_By"                        as created_by,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_partner raw
