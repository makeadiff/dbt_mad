{{ config(materialized='table') }}

-- bridge_poc_partner: Mapping between Points of Contact and Partners
-- Flow: int_crm__poc_partners → bridge_poc_partner

select
    poc_partner_id,
    poc_id,
    partner_id,
    created_at,
    updated_at
from {{ ref('int_crm__poc_partners') }}
