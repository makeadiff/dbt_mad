{{ config(materialized='table') }}

-- fct_meetings: One row per meeting event
-- Flow: int_crm__meetings → fct_meetings

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
    case when follow_up_meeting_scheduled = 'Yes' then true else false end as is_follow_up_scheduled,
    created_at,
    updated_at
from {{ ref('int_crm__meetings') }}
