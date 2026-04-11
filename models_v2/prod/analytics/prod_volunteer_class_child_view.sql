{{ config(materialized='table') }}

-- Volunteer Class Child View: volunteer→class→child operational view
-- Optimized: Uses shared int_bubble__volunteer_class_child_detail

-- TODO: Integrate Platform Commons (PC) data for operational Volunteer-to-Child mapping
SELECT 
    volunteer_sk,
    slot_sk,
    class_section_sk,
    volunteer_id,
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
    subject_name,
    slot_class_section_id
FROM {{ ref('int_bubble__volunteer_class_child_detail') }}
WHERE volunteer_is_removed = FALSE
  AND scs_is_active = TRUE
