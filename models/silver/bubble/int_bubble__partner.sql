{{ config(materialized='table') }}

-- Deduplicated Bubble partners
-- Source: stg_bubble__partner

with source_data as (
    select * from {{ ref('stg_bubble__partner') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='partner_id1',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['partner_id1']) }} as partner_sk,
    partner_id1 as partner_id,
    partner_id1 as bubble_partner_id,
    partner_id as bubble_partner_uuid,
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
    created_by,
    co_id as co_id_user,
    mou_url,
    city_id,
    poc_email,
    state_id,
    classes_list,
    partner_id1,
    poc_contact,
    poc_designation,
    date_of_first_contact,
    low_income_resource,
    partner_affiliation_type
from deduplicated
