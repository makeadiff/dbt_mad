{{ config(materialized='table') }}

-- Deduplicated Bubble users (volunteers)
-- Source: stg_bubble__user

with source_data as (
    select * from {{ ref('stg_bubble__user') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='user_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id_number']) }} as volunteer_sk,
    user_id,
    city,
    state,
    center,
    user_id_number as volunteer_id,
    user_role,
    user_display_name,
    authentication,
    contact_number,
    user_login,
    user_signed_up,
    updated_password,
    reporting_manager_role_code,
    created_date,
    modified_date
from deduplicated
