{{ config(materialized='table') }}

-- dim_mou: Memorandum of Understanding details
-- Flow: int_crm__mous → dim_mou

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
    created_at,
    updated_at
from {{ ref('int_crm__mous') }}
