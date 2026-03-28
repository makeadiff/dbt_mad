{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'poc_partners') }}
)
select
    id::integer as poc_partner_id,
    poc_id::integer as poc_id,
    partner_id::integer as partner_id,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
