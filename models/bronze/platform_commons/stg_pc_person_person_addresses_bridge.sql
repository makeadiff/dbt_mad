{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'person_personAddresses_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "personId"::bigint as person_id,
    "personAddressesId"::bigint as person_address_id
from raw
