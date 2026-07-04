{{ config(materialized='table') }}

-- School Volunteer: volunteer assignments with partner and user details
-- Sourced from Platform Commons user data for volunteer profiles

SELECT
    -- School volunteer base columns
    sv.school_volunteer_id,
    sv.academic_year,
    sv.school_id,
    sv.volunteer_id,
    sv.is_removed AS removed,
    sv.assigned_date::timestamp without time zone as created_date,
    sv.modified_date::timestamp without time zone as modified_date,
    
    -- Partner data columns
    p.bubble_partner_id AS partner_id,
    p.city AS partner_city,
    p.partner_created_by,
    p.partner_co_id_user,
    p.state AS partner_state,
    p.created_date AS partner_created_date,
    p.co_name AS partner_co_name,
    p.mou_url,
    p.modified_date AS partner_modified_date,
    p.poc_name,
    p.partner_city_id,
    p.pincode,
    p.poc_email,
    p.partner_state_id,
    p.lead_source,
    p.school_type,
    p.classes_list,
    p.mou_end_date,
    p.partner_name,
    p.mou_sign_date,
    p.partner_id1,
    p.poc_contact,
    p.address_line_1,
    p.address_line_2,
    p.mou_start_date,
    p.poc_designation,
    p.total_child_count,
    p.date_of_first_contact,
    p.low_income_resource,
    p.confirmed_child_count,
    p.partner_affiliation_type,
    
    -- User data columns
    u.user_key AS user_id,
    u."City" AS user_city,
    u."State" AS user_state,
    u."Center" AS user_center,
    u."UserCreatedDateTime" AS user_created_date,
    u."UserUpdatedDateTime" AS user_modified_date,
    CAST(NULL AS text) AS authentication,
    u."Contact" AS contact_number,
    u."UserId" AS user_id_number,
    u."UserRole" AS user_role,
    CAST(NULL AS timestamp) AS user_signed_up,
    u."UserLogin" AS user_login,
    CAST(NULL AS text) AS updated_password,
    u."UserDisplayName" AS user_display_name,
    u."ReportingManagerRoleCode" AS reporting_manager_role_code

FROM {{ ref('fct_school_volunteer') }} sv
LEFT JOIN {{ ref('dim_bubble_partner') }} p ON sv.school_id = p.bubble_partner_id
LEFT JOIN {{ ref('int_pc_user_data') }} u ON sv.volunteer_id::numeric = u."UserId"::numeric
