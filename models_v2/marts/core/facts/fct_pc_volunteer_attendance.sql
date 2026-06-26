{{ config(materialized='table') }}

select
    -- Surrogate keys
    volunteer_attendance_key,
    session_key,
    volunteer_key,

    -- Session context
    stream,
    course,
    city_name,
    center_name,
    school_id,
    center_slot_id,
    center_slot_name,
    center_slot_day_of_week,
    center_slot_start_time,
    center_slot_end_time,
    class_name,
    section,
    section_id,
    section_slot_shift_id,
    slot_mentor_id,
    slot_mentor_name,
    subject_code,
    scheduled_session_date,

    -- Tagged volunteer
    attendance_id,
    tagged_volunteer_id,
    tagged_volunteer_name,
    is_attendance_taken_for_tagged_volunteer,
    volunteer_name,
    attendance_status,
    zero_attendance_status,

    -- Substitution info
    assignee_user_name,
    substitute_id,
    assignee_user_id,
    substituted_volunteer_user_id,
    substituted_volunteer_user_name,
    request_status,
    substitution_type,
    substitution_reason,

    -- Class attendance metadata
    class_attendance_taken_by_user_id,
    class_attendance_taken_by_user_name,
    substitute_volunteer_attendance_status,

    -- Identifiers
    class_id

from {{ ref('int_pc_volunteer_attendance') }}
