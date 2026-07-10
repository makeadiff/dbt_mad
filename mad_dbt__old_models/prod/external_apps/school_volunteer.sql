{{ config(materialized='table') }}

with school_volunteer_data as (
    select * from {{ ref('school_volunteer_int') }}
),
user_data as (
    select * from {{ ref('user_int') }}
),
partner_data as (
    select * from {{ ref('partner_int') }}
)
select
    -- School volunteer base columns
    sv.school_volunteer_id,
    sv.academic_year,
    sv.school_id,
    sv.volunteer_id,
    sv.removed,
    sv.created_date::timestamp without time zone as created_date,
    sv.modified_date::timestamp without time zone as modified_date,
    
    -- Partner data columns
    p.partner_id,
    p.city as partner_city,
    p.created_by as partner_created_by,
    p.co_id_user as partner_co_id_user,
    p.state as partner_state,
    p.created_date as partner_created_date,
    p.co_name as partner_co_name,
    p.mou_url,
    p.modified_date as partner_modified_date,
    p.poc_name,
    p.city_id as partner_city_id,
    p.pincode,
    p.poc_email,
    p.state_id as partner_state_id,
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
    u.user_id,
    u.city as user_city,
    u.state as user_state,
    u.center as user_center,
    u.created_date as user_created_date,
    u.modified_date as user_modified_date,
    u.authentication,
    u.contact_number,
    u.user_id_number,
    u.user_role,
    u.user_signed_up,
    u.user_login,
    u.updated_password,
    u.user_display_name,
    u.reporting_manager_role_code
from school_volunteer_data sv
left join partner_data p on sv.school_id = p.partner_id1
left join user_data u on sv.volunteer_id = u.user_id_number