{{ config(materialized='table') }}

-- dim_volunteer: One row per volunteer
-- Source: int_bubble__user

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as volunteer_sk,
    user_id as volunteer_id,
    user_display_name as volunteer_name,
    user_role,
    city,
    state,
    center,
    created_date,
    modified_date
from {{ ref('int_bubble__user') }}
