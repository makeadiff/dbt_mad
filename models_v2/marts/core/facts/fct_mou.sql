-- fct_mou: One row per MOU
-- Grain: One record per Memorandum of Understanding between org and partner

select
    mou_sk,
    partner_sk,
    mou_id,
    partner_id,
    mou_url,
    mou_sign,
    mou_status,
    mou_start_date,
    mou_end_date,
    mou_sign_date,
    pending_mou_reason,
    confirmed_child_count,
    mou_end_date::date - mou_start_date::date as mou_duration_days,
    case 
        when mou_end_date >= current_date 
        then (mou_end_date::date - current_date)
        else 0
    end as days_until_expiry,
    case when mou_end_date >= current_date then true else false end as is_active_mou,
    created_at,
    updated_at
from {{ ref('int_crm__mous') }}
