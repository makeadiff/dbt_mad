{{ config(materialized='table') }}

-- School Volunteer: volunteer assignments with partner and user details

-- TODO: Integrate Platform Commons (PC) data for Volunteer-to-Partner/School mapping
SELECT
    sv.school_volunteer_sk,
    sv.school_volunteer_id,
    sv.academic_year,
    sv.school_id,
    sv.volunteer_id,
    sv.days_assigned,
    sv.is_active,
    sv.is_removed,
    sv.assigned_date,
    sv.modified_date,
    
    -- Partner details
    p.bubble_partner_id AS partner_id,
    p.city AS partner_city,
    p.co_name AS partner_co_name,
    p.state AS partner_state,
    p.created_date AS partner_created_date,
    p.mou_sign_date,
    p.modified_date AS partner_modified_date,
    p.poc_name,
    p.pincode,
    p.lead_source,
    p.school_type,
    p.mou_start_date,
    p.mou_end_date,
    p.partner_name,
    p.address_line_1,
    p.address_line_2,
    p.total_child_count,
    p.confirmed_child_count,
    
    -- User details
    -- TODO: Integrate Platform Commons (PC) data for Volunteer profiles
    v.volunteer_id AS user_id,
    v.city AS user_city,
    v.state AS user_state,
    v.center AS user_center,
    v.created_date AS user_created_date,
    v.modified_date AS user_modified_date,
    v.authentication,
    v.contact_number,
    v.user_role,
    v.user_signed_up,
    v.user_login,
    v.updated_password,
    v.volunteer_name AS user_display_name,
    v.reporting_manager_role_code

FROM {{ ref('fct_school_volunteer') }} sv
JOIN {{ ref('dim_bubble_partner') }} p ON sv.school_id = p.bubble_partner_id
JOIN {{ ref('dim_volunteer') }} v ON sv.volunteer_id = v.volunteer_id
