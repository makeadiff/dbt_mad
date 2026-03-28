{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'cities') }}
)
select
    id::integer as city_id,
    state_id::integer as state_id,
    city_name,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
