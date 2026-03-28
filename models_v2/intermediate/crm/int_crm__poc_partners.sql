{{ config(materialized='table') }}

-- Deduplicated POC to Partner relationship assignments

with source_data as (
    select * from {{ ref('stg_crm__poc_partners') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='poc_partner_id',
        order_by='updated_at desc',
       )
    }}
)

select
    poc_partner_id::text,
    poc_id::text,
    partner_id::text,
    created_at,
    updated_at
from deduplicated
