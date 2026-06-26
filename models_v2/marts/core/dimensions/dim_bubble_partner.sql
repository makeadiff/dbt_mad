{{ config(materialized='table') }}

-- dim_bubble_partner: One row per Bubble partner/school
-- Flow: stg_bubble__partner → dim_bubble_partner

select
    partner_sk as bubble_partner_sk,
    partner_id as bubble_partner_id,
    bubble_partner_uuid,
    partner_name,
    city,
    state,
    co_name,
    poc_name,
    school_type,
    lead_source,
    pincode,
    address_line_1,
    address_line_2,
    mou_sign_date,
    mou_start_date,
    mou_end_date,
    total_child_count,
    confirmed_child_count,
    is_removed,
    created_date,
    modified_date,
    created_by as partner_created_by,
    co_id_user as partner_co_id_user,
    mou_url,
    city_id as partner_city_id,
    poc_email,
    state_id as partner_state_id,
    classes_list,
    partner_id1,
    poc_contact,
    poc_designation,
    date_of_first_contact,
    low_income_resource,
    partner_affiliation_type
from {{ ref('int_bubble__partner') }}
where is_removed = false
