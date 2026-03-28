{{ config(materialized='table') }}

-- Deduplicated partner organizations and schools

with source_data as (
    select * from {{ ref('stg_crm__partners') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='partner_id',
        order_by='updated_at desc',
       )
    }}
)

select
    partner_id::text,
    partner_name,
    city_id::text,
    state_id::text,
    pincode,
    is_removed,
    interested,
    lead_source,
    school_type,
    partner_affiliation_type,
    address_line_1,
    address_line_2,
    total_child_count,
    low_income_resource,
    classes,
    created_by::text,
    created_at,
    updated_at
from deduplicated
