-- fct_meetings: One row per meeting event
-- Grain: One record per meeting between MAD staff and partner contacts

select
    meeting_sk,
    partner_sk,
    poc_sk,
    user_sk,
    meeting_id,
    partner_id,
    poc_id,
    user_id,
    meeting_date,
    follow_up_meeting_date,
    follow_up_meeting_scheduled,
    case when follow_up_meeting_scheduled = 'Yes' then true else false end as is_follow_up_scheduled,
    case 
        when follow_up_meeting_date is not null 
        then (follow_up_meeting_date::date - meeting_date::date)
    end as days_to_follow_up,
    created_at,
    updated_at
from {{ ref('int_crm__meetings') }}
