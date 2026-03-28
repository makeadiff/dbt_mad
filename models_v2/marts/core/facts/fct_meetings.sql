{{ config(materialized='table') }}

-- fct_meetings: One row per meeting event
-- Flow: int_crm__meetings → fct_meetings

select
    meeting_id,
    partner_id,
    poc_id,
    user_id,
    meeting_date,
    follow_up_meeting_date,
    follow_up_meeting_scheduled,
    created_at,
    updated_at
from {{ ref('int_crm__meetings') }}
