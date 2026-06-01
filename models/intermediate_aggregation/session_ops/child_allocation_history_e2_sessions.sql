{{ config(materialized='table') }}

-- Child allocation at child + slot_class_section grain.
-- Join path based on prod analytics volunteer_allocation_history; volunteer columns removed.

SELECT
  ch.child_id,
  ch.first_name AS child_first_name,
  ch.last_name AS child_last_name,
  scs.slot_class_section_id,
  cs.section_name,
  c.class_name,
  sub.subject_name,
  p.id AS partner_id,
  p.partner_name,
  s.day_of_week,
  ay.label AS academic_year,
  COALESCE(
    ssd.start_date,
    CASE say.academic_year_id
      WHEN 1 THEN '2025-07-15'::timestamp
      WHEN 2 THEN '2026-07-15'::timestamp
      ELSE NULL
    END
  ) AS sessions_start_date
FROM {{ ref('child_class_section_int') }} ccs
JOIN {{ ref('child_int') }} ch
  ON ccs.child_id = ch.child_id
JOIN {{ ref('class_section_int') }} cs
  ON ccs.class_section_id = cs.class_section_id
JOIN {{ ref('slot_class_section_int') }} scs
  ON cs.class_section_id = scs.class_section_id
JOIN {{ ref('slot_int') }} s
  ON scs.slot_id = s.slot_id
JOIN {{ ref('school_class_int') }} sc
  ON cs.school_class_id = sc.school_class_id
JOIN {{ ref('class_int') }} c
  ON sc.class_id = c.class_id
JOIN {{ ref('class_section_subject_int') }} css
  ON scs.class_section_subject_id = css.class_section_subject_id
JOIN {{ ref('subject_int') }} sub
  ON css.subject_id = sub.subject_id
  AND sub.removed = FALSE
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
  AND cs.is_active = TRUE
  AND p.removed = FALSE
  AND ccs.removed_boolean = FALSE
