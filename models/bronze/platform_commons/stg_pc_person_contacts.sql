{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'personContacts') }}
)

select
    id::bigint as person_contact_id,
    "contactId"::bigint as contact_id,
    "isPrimary"::boolean as is_primary,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
