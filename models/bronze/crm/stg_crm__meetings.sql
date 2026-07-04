{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'meetings') }}
)
select
    id::integer as meeting_id,
    partner_id::integer as partner_id,
    poc_id::integer as poc_id,
    user_id::integer as user_id,
    meeting_date::date as meeting_date,
    follow_up_meeting_date::date as follow_up_meeting_date,
    follow_up_meeting_scheduled::boolean as follow_up_meeting_scheduled,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
