{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'mous') }}
)
select
    id::integer as mou_id,
    partner_id::integer as partner_id,
    mou_url,
    mou_sign::boolean as mou_sign,
    mou_status,
    mou_start_date::date as mou_start_date,
    mou_end_date::date as mou_end_date,
    mou_sign_date::date as mou_sign_date,
    pending_mou_reason,
    confirmed_child_count::integer as confirmed_child_count,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
