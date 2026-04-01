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
    user_id,
    city,
    state,
    center,
    user_id_number,
    user_role,
    user_display_name,
    reporting_manager_role_code,
    created_date,
    modified_date
from deduplicated
