{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'states') }}
)
select
    id::integer as state_id,
    state_name,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
