{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'person_personContacts_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "personId"::bigint as person_id,
    "personContactsId"::bigint as person_contact_id
from raw
