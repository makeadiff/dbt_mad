{{ config(materialized='table') }}

-- Deduplicated meeting records between staff and partner contacts

with source_data as (
    select * from {{ ref('stg_crm__meetings') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='meeting_id',
        order_by='updated_at desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['meeting_id']) }} as meeting_sk,
    {{ dbt_utils.generate_surrogate_key(['partner_id']) }} as partner_sk,
    {{ dbt_utils.generate_surrogate_key(['poc_id']) }} as poc_sk,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
    meeting_id,
    partner_id,
    poc_id,
    user_id,
    meeting_date,
    follow_up_meeting_date,
    follow_up_meeting_scheduled,
    created_at,
    updated_at
from deduplicated
