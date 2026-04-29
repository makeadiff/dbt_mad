{{ config(materialized='table') }}

-- Deduplicated partner agreement pipeline and conversion tracking

with source_data as (
    select * from {{ ref('stg_crm__partner_agreements') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='partner_agreement_id',
        order_by='updated_at desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['partner_agreement_id']) }} as partner_agreement_sk,
    {{ dbt_utils.generate_surrogate_key(['partner_id']) }} as partner_sk,
    partner_agreement_id,
    partner_id,
    current_status,
    conversion_stage,
    specific_doc_name,
    specific_doc_required,
    agreement_drop_date,
    non_conversion_reason,
    potential_child_count,
    expected_conversion_day,
    is_removed,
    created_at,
    updated_at
from deduplicated
