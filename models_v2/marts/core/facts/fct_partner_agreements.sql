{{ config(materialized='table') }}

-- fct_partner_agreements: One row per partner agreement state tracking record
-- Flow: int_crm__partner_agreements → fct_partner_agreements

select
    {{ dbt_utils.generate_surrogate_key(['partner_agreement_id']) }} as agreement_sk,
    {{ dbt_utils.generate_surrogate_key(['partner_id']) }} as partner_sk,
    partner_agreement_id as agreement_id,
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
from {{ ref('int_crm__partner_agreements') }}
where is_removed = false
