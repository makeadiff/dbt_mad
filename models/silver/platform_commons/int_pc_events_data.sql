{{ config(materialized='table') }}

with applicant as (
    select * from {{ ref('stg_pc_opportunity_applicant') }}
),

opportunity as (
    select * from {{ ref('stg_pc_opportunity') }}
),

users as (
    select * from {{ ref('stg_pc_user') }}
)

select
    {{ clean_prefix('o.opportunity_sub_type_code') }} as event_type,
    a.user_id as volunteer_id,
    a.opportunity_id,
    u.first_name || ' ' || coalesce(u.last_name, '') as volunteer_name,
    u.created_datetime as user_sign_up_date,
    u.login as volunteer_email,
    o.opportunity_name,
    {{ clean_prefix('a.application_status') }} as attendance_status, -- Mapping status as attendance in this context
    a.application_datetime as invitation_date_time, -- Approximating invitation as application date
    a.application_datetime,
    u.updated_datetime as user_updated_date_time,
    a.attendance_marked_on as attendance_marked_on_date,
    a.opportunity_applicant_id
from applicant a
join opportunity o on a.opportunity_id = o.opportunity_id
join users u on a.user_id = u.user_id
where o.opportunity_type_code = 'OPPORTUNITY_TYPE.EVENT'
