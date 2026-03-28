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
    meeting_id::text,
    partner_id::text,
    poc_id::text,
    user_id::text,
    meeting_date,
    follow_up_meeting_date,
    follow_up_meeting_scheduled,
    created_at,
    updated_at
from deduplicated
