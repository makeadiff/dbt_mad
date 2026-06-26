{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partner_cos') }}
)
select
    id::integer as partner_co_id,
    partner_id::integer as partner_id,
    co_id::integer as co_id,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
