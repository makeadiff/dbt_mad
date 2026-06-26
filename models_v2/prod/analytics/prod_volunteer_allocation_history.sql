{{ config(materialized='table') }}

-- Volunteer Allocation History: volunteer-slot allocations with rich context
-- Optimized: Uses shared int_bubble__volunteer_class_child_detail

-- TODO: Integrate Platform Commons (PC) data for historical volunteer allocation tracking
SELECT
    volunteer_sk,
    slot_sk,
    class_section_sk,
    volunteer_id,
    slot_class_section_id,
    created_date AS start_date,
    CASE WHEN volunteer_is_removed THEN volunteer_modified_date END AS end_date,
    NOT volunteer_is_removed AS is_active,
    slot_id,
    class_section_subject_id,
    section_name,
    partner_id,
    partner_name,
    user_id,
    user_display_name,
    contact,
    user_login,
    day_of_week,
    slot_name,
    child_id,
    child_first_name,
    child_last_name,
    class_name,
    subject_name
FROM {{ ref('int_bubble__volunteer_class_child_detail') }}
ORDER BY volunteer_id, created_date
