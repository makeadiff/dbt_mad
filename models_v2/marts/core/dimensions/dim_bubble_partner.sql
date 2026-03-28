{{ config(materialized='table') }}

-- dim_bubble_partner: One row per Bubble partner/school
-- Flow: stg_bubble__partner → dim_bubble_partner

select
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
    modified_date
from {{ ref('stg_bubble__partner') }}
where is_removed = false
