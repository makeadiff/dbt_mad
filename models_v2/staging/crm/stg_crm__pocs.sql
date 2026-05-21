{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'pocs') }}
)
select
    id::integer as poc_id,
    partner_id::integer as partner_id,
    poc_name,
    poc_email,
    poc_contact,
    poc_designation,
    date_of_first_contact::date as date_of_first_contact,
    removed::boolean as is_removed,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
