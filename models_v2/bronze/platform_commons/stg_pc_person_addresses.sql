{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'personAddresses') }}
)

select
    id::bigint as person_address_id,
    "addressId"::bigint as address_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
