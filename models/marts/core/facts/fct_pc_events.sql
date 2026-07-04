{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['opportunity_applicant_id']) }} as event_attendance_key,
    {{ dbt_utils.generate_surrogate_key(['volunteer_id']) }} as user_key,
    {{ dbt_utils.generate_surrogate_key(['opportunity_id']) }} as opportunity_key,
    opportunity_applicant_id,
    volunteer_id,
    opportunity_id,
    event_type,
    opportunity_name,
    attendance_status,
    invitation_date_time,
    application_datetime,
    attendance_marked_on_date
from {{ ref('int_pc_events_data') }}
