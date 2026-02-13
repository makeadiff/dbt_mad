{{ config(materialized='table') }}
-- Volunteer Allocation History: Tracks volunteer-slot allocations over time
-- Includes start/end dates, active flag, and rich context from related entities

SELECT
  scsv.volunteer_id,
  scsv.slot_class_section_id,
  scsv.created_date AS start_date,
  CASE
    WHEN scsv.removed = TRUE THEN scsv.modified_date
    ELSE NULL
  END AS end_date,
  -- Mark whether record is still active
  CASE WHEN scsv.removed = TRUE THEN FALSE ELSE TRUE END AS is_active,
  scs.slot_id,
  scs.class_section_subject_id,
  cs.section_name,
  p.id AS partner_id,
  p.partner_name,
  ud.user_id,
  ud.user_display_name,
  ud.contact,
  ud.email,
  ud.user_login,
  s.day_of_week,
  s.slot_name,
  ch.child_id,
  ch.first_name AS child_first_name,
  ch.last_name AS child_last_name,
  c.class_name,
  sub.subject_name,
  now() AT TIME ZONE 'Asia/Kolkata' AS dbt_transformed_at
FROM {{ ref('slot_class_section_volunteer_int') }} scsv
JOIN {{ ref('slot_class_section_int') }} scs
  ON scsv.slot_class_section_id = scs.slot_class_section_id
JOIN {{ ref('class_section_int') }} cs
  ON scs.class_section_id = cs.class_section_id
JOIN {{ ref('user_data_int') }} ud
  ON scsv.volunteer_id::text = ud.user_id
JOIN {{ ref('slot_int') }} s
  ON scs.slot_id = s.slot_id
JOIN {{ ref('school_class_int') }} sc
  ON cs.school_class_id = sc.school_class_id
JOIN {{ ref('class_int') }} c
  ON sc.class_id = c.class_id
JOIN {{ ref('class_section_subject_int') }} css
  ON scs.class_section_subject_id = css.class_section_subject_id
  AND css.removed = FALSE
JOIN {{ ref('subject_int') }} sub
  ON css.subject_id = sub.subject_id
  AND sub.removed = FALSE
JOIN {{ ref('child_class_section_int') }} ccs
  ON cs.class_section_id = ccs.class_section_id
  AND ccs.removed_boolean = FALSE
JOIN {{ ref('child_int') }} ch
  ON ccs.child_id = ch.child_id
JOIN {{ ref('partners_int') }} p
  ON cs.school_id = p.id::numeric
-- We include both removed = TRUE/FALSE → for historical + active
WHERE
  scs.removed = FALSE
  AND cs.removed = FALSE
  AND cs.is_active = TRUE
  AND p.removed = FALSE
ORDER BY scsv.volunteer_id, start_date


