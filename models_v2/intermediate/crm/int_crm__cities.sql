{{ config(materialized='table') }}

-- Deduplicated city master data linked to states

with source_data as (
    select * from {{ ref('stg_crm__cities') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='city_id',
        order_by='updated_at desc',
       )
    }}
)

select
    city_id::text,
    state_id::text,
    city_name,
    created_at,
    updated_at
from deduplicated
