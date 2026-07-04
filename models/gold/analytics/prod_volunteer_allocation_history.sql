{{ config(materialized='table') }}

-- Volunteer Allocation History: volunteer-slot allocations with rich context
-- Sourced from Platform Commons user data for volunteer profiles

SELECT
    d.volunteer_sk,
    d.slot_sk,
    d.class_section_sk,
    d.volunteer_id,
    d.slot_class_section_id,
    d.created_date AS start_date,
    CASE WHEN d.volunteer_is_removed THEN d.volunteer_modified_date END AS end_date,
    NOT d.volunteer_is_removed AS is_active,
    d.slot_id,
    d.class_section_subject_id,
    d.section_name,
    d.partner_id,
    d.partner_name,
    ud."UserId" AS user_id,
    ud."UserDisplayName" AS user_display_name,
    ud."Contact" AS contact,
    ud."Email" AS email,
    ud."UserLogin" AS user_login,
    d.day_of_week,
    d.slot_name,
    d.child_id,
    d.child_first_name,
    d.child_last_name,
    d.class_name,
    d.subject_name
FROM {{ ref('int_bubble__volunteer_class_child_detail') }} d
LEFT JOIN {{ ref('int_pc_user_data') }} ud ON d.volunteer_id::numeric = ud."UserId"::numeric
ORDER BY d.volunteer_id, start_date
