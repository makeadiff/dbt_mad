{{ config(materialized='table') }}

-- bridge_partner_co: Mapping between Partners and Community Organizers
-- Flow: int_crm__partner_cos → bridge_partner_co

select
    partner_co_id,
    partner_id,
    co_id,
    created_at,
    updated_at
from {{ ref('int_crm__partner_cos') }}
