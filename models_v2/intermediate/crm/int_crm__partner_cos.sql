{{ config(materialized='table') }}

-- Deduplicated partner to Community Organizer assignments

with source_data as (
    select * from {{ ref('stg_crm__partner_cos') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='partner_co_id',
        order_by='updated_at desc',
       )
    }}
)

select
    partner_co_id::text,
    partner_id::text,
    co_id::text,
    created_at,
    updated_at
from deduplicated
