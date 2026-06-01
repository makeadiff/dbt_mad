{{ config(materialized='table') }}

-- Volunteer allocation history at volunteer–slot grain (no child expansion).
-- Based on prod analytics volunteer_allocation_history; child/class/subject joins removed.

SELECT
  scsv.volunteer_id,
  scsv.slot_class_section_id,
  scsv.created_date AS slot_class_sec_start_date,
  CASE
    WHEN scsv.removed = TRUE THEN scsv.modified_date
    ELSE NULL
  END AS slot_class_sec_end_date,
  CASE WHEN scsv.removed = FALSE THEN TRUE ELSE FALSE END AS is_active,
  scs.slot_id,
  scs.class_section_subject_id,
  cs.section_name,
  p.id AS partner_id,
  p.partner_name,
  ud.user_id,
  ud.user_display_name,
  s.day_of_week,
  s.slot_name,
  ay.label AS academic_year,
  COALESCE(
    ssd.start_date,
    CASE say.academic_year_id
      WHEN 1 THEN '2025-07-15'::timestamp
      WHEN 2 THEN '2026-07-15'::timestamp
      ELSE NULL
    END
  ) AS sessions_start_date
FROM {{ ref('slot_class_section_volunteer_int') }} scsv
JOIN {{ ref('slot_class_section_int') }} scs
  ON scsv.slot_class_section_id = scs.slot_class_section_id
JOIN {{ ref('class_section_int') }} cs
  ON scs.class_section_id = cs.class_section_id
LEFT JOIN {{ ref('user_data_int') }} ud
  ON scsv.volunteer_id::text = ud.user_id
JOIN {{ ref('slot_int') }} s
  ON scs.slot_id = s.slot_id
LEFT JOIN {{ ref('school_academic_year_int') }} say
  ON s.school_academic_year_id = say.school_academic_year_id
LEFT JOIN {{ ref('academic_year_int') }} ay
  ON say.academic_year_id = ay.academic_year_id
LEFT JOIN {{ ref('school_session_detail_int') }} ssd
  ON say.school_academic_year_id = ssd.school_academic_year_id
  AND cs.school_id = ssd.school_id
  AND ssd.removed = FALSE
JOIN {{ ref('partners_int') }} p
  ON cs.school_id::text = p.id
WHERE
  scs.removed = FALSE
  AND cs.removed = FALSE
  AND p.removed = FALSE
