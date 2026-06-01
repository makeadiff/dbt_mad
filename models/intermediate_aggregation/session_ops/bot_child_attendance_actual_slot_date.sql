{{ config(materialized='table') }}

WITH source AS (
  SELECT
    partner_id,
    partner_name,
    child_id,
    child_name,
    slot_class_section_id,
    attendance,
    day_of_week,
    entry_last_updated_date
  FROM {{ ref('dots_child_attendance_int') }}
),

with_submission_date AS (
  SELECT
    *,
    entry_last_updated_date::date AS submission_date
  FROM source
),

with_slot_dow AS (
  SELECT
    *,
    CASE TRIM(day_of_week)
      WHEN 'Sunday' THEN 0
      WHEN 'Monday' THEN 1
      WHEN 'Tuesday' THEN 2
      WHEN 'Wednesday' THEN 3
      WHEN 'Thursday' THEN 4
      WHEN 'Friday' THEN 5
      WHEN 'Saturday' THEN 6
      ELSE NULL
    END AS slot_dow
  FROM with_submission_date
),

with_snapped_date AS (
  SELECT
    *,
    CASE
      WHEN slot_dow IS NOT NULL AND submission_date IS NOT NULL
      THEN submission_date - (
        (EXTRACT(DOW FROM submission_date)::int - slot_dow + 7) % 7
      )
      ELSE NULL
    END AS snapped_date
  FROM with_slot_dow
)

SELECT
  wsd.partner_id,
  wsd.partner_name,
  wsd.child_id,
  wsd.child_name,
  wsd.slot_class_section_id,
  wsd.attendance,
  wsd.day_of_week,
  wsd.entry_last_updated_date,
  wsd.snapped_date::timestamp AS date_of_slot,
  ay.label AS academic_year
FROM with_snapped_date wsd
LEFT JOIN {{ ref('slot_class_section_int') }} scs
  ON wsd.slot_class_section_id = scs.slot_class_section_id
LEFT JOIN {{ ref('slot_int') }} s
  ON scs.slot_id = s.slot_id
LEFT JOIN {{ ref('school_academic_year_int') }} say
  ON s.school_academic_year_id = say.school_academic_year_id
LEFT JOIN {{ ref('academic_year_int') }} ay
  ON say.academic_year_id = ay.academic_year_id
