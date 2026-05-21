{{ config(materialized='table') }}

select
    volunteer_attendance_key,
    session_key,
    volunteer_key,
    attendance_id,
    attendance_date,
    attendance_status,
    zero_attendance_status,
    center_slot_id,
    substitute_id,
    substitute_user_id,
    request_status,
    substitution_type,
    substitution_reason,
    substituted_volunteer_user_id
from {{ ref('int_pc_volunteer_attendance') }}
