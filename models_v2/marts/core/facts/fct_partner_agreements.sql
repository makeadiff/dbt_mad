-- fct_partner_agreements: One row per partner agreement state tracking record
-- Grain: One record per partner per stage in the agreement conversion funnel

select
    partner_agreement_sk as agreement_sk,
    partner_sk,
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
    case 
        when conversion_stage = 'converted' and agreement_drop_date is not null 
        then (agreement_drop_date::date - created_at::date) 
    end as days_to_conversion,
    case when conversion_stage = 'converted' then true else false end as is_converted,
    is_removed,
    created_at,
    updated_at
from {{ ref('int_crm__partner_agreements') }}
where is_removed = false
