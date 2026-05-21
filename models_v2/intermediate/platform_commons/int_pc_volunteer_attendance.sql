{{ config(materialized='table') }}

with attendance as (
    select * from {{ ref('stg_pc_volunteer_attendance') }}
),
substitute as (
    select * from {{ ref('stg_pc_substitute') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['a.attendance_id']) }} as volunteer_attendance_key,
    {{ dbt_utils.generate_surrogate_key(['a.for_slot_shift_id', 'a.attendance_date']) }} as session_key,
    {{ dbt_utils.generate_surrogate_key(['a.community_member_id']) }} as volunteer_key,

    a.attendance_id,
    a.community_member_id as volunteer_id,
    a.attendance_date,
    a.attendance_status,
    a.zero_attendance_status,
    a.for_slot_shift_id as center_slot_id,
    
    s.substitute_id,
    s.by_user_id as substitute_user_id,
    s.request_status,
    s.request_type as substitution_type,
    s.requesting_reason as substitution_reason,
    s.for_user_id as substituted_volunteer_user_id
    
from attendance a
left join substitute s on a.for_slot_shift_id = s.for_slot_shift_id and a.attendance_date = s.for_date
