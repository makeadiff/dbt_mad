{{ config(materialized='table') }}

-- dim_volunteer: One row per volunteer
-- Source: int_bubble__user

select
    volunteer_sk,
    volunteer_id,
    user_display_name as volunteer_name,
    user_role,
    authentication,
    contact_number,
    user_login,
    user_signed_up,
    updated_password,
    city,
    state,
    center,
    reporting_manager_role_code,
    created_date,
    modified_date
from {{ ref('int_bubble__user') }}
