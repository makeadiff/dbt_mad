{{ config(materialized='table') }}

-- Volunteer Class Child View: volunteer→class→child operational view
-- Optimized: dim_class_section already has class_name (eliminates school_class→class join)

-- TODO: Integrate Platform Commons (PC) data for operational Volunteer-to-Child mapping
SELECT 
    scsv.volunteer_sk,
    scs.slot_sk,
    cs.class_section_sk,
    scsv.volunteer_id,
    scs.slot_id,
    scs.class_section_subject_id,
    cs.section_name,
    p.crm_partner_id AS partner_id,
    p.partner_name,
    v.volunteer_id AS user_id,
    v.volunteer_name AS user_display_name,
    v.contact_number AS contact,
    v.user_login, 
    s.day_of_week,
    s.slot_name,
    ch.child_id,
    ch.first_name AS child_first_name,
    ch.last_name AS child_last_name,
    cs.class_name,
    sub.subject_name,
    scs.slot_class_section_id
FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
JOIN {{ ref('int_bubble__slot_class_section') }} scs ON scsv.slot_class_section_id = scs.slot_class_section_id
JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
JOIN {{ ref('dim_volunteer') }} v ON scsv.volunteer_id = v.volunteer_id
JOIN {{ ref('dim_slot') }} s ON scs.slot_id = s.slot_id
JOIN {{ ref('int_bubble__class_section_subject') }} css
  ON scs.class_section_subject_id = css.class_section_subject_id AND css.is_removed = FALSE
JOIN {{ ref('dim_subject') }} sub ON css.subject_id = sub.subject_id
JOIN {{ ref('bridge_child_class_section') }} ccs
  ON cs.class_section_id = ccs.class_section_id AND ccs.is_removed = FALSE
JOIN {{ ref('dim_child') }} ch ON ccs.child_id = ch.child_id
JOIN {{ ref('dim_crm_partner') }} p ON cs.school_id = p.crm_partner_id
WHERE scsv.is_removed = FALSE
  AND scs.is_removed = FALSE
  AND scs.is_active = TRUE
  AND cs.is_active = TRUE
