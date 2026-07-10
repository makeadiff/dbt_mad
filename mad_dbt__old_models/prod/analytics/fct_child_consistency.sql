{{ config(materialized='table') }}

WITH sessions_happened_per_section AS (
  SELECT
    slot_class_section_id,
    academic_year,
    COUNT(DISTINCT date_of_slot) AS sessions_happened
  FROM {{ ref('bot_attendance_actual_slot_date') }}
  GROUP BY
    slot_class_section_id,
    academic_year
),

child_attendance_agg AS (
  SELECT
    child_id,
    slot_class_section_id,
    academic_year,
    COUNT(DISTINCT date_of_slot) AS attended_sessions
  FROM {{ ref('bot_child_attendance_actual_slot_date') }}
  GROUP BY
    child_id,
    slot_class_section_id,
    academic_year
),

joined AS (
  SELECT
    ca.child_id,
    ca.child_first_name,
    ca.child_last_name,
    ca.partner_id AS chapter_id,
    ca.partner_name AS chapter_name,
    ca.academic_year,
    COALESCE(sh.sessions_happened, 0) AS sessions_happened,
    COALESCE(aa.attended_sessions, 0) AS attended_sessions
  FROM {{ ref('child_allocation_history_e2_sessions') }} ca
  LEFT JOIN sessions_happened_per_section sh
    ON ca.slot_class_section_id = sh.slot_class_section_id
    AND ca.academic_year = sh.academic_year
  LEFT JOIN child_attendance_agg aa
    ON ca.child_id = aa.child_id
    AND ca.slot_class_section_id = aa.slot_class_section_id
    AND ca.academic_year = aa.academic_year
),

child_aggregated AS (
  SELECT
    child_id,
    child_first_name,
    child_last_name,
    chapter_id,
    chapter_name,
    academic_year,
    SUM(sessions_happened) AS sessions_happened,
    SUM(attended_sessions) AS attended_sessions
  FROM joined
  GROUP BY
    child_id,
    child_first_name,
    child_last_name,
    chapter_id,
    chapter_name,
    academic_year
)

SELECT
  child_id,
  child_first_name,
  child_last_name,
  chapter_id,
  chapter_name,
  academic_year,
  sessions_happened,
  attended_sessions,
  -- TODO: update multiplier when actual session duration per slot is available.
  attended_sessions * 2 AS hours_of_support,
  CASE
    WHEN sessions_happened = 0 THEN NULL
    ELSE ROUND(
      attended_sessions::numeric / NULLIF(sessions_happened, 0) * 100,
      1
    )
  END AS attendance_pct,
  CASE
    WHEN sessions_happened = 0 THEN 'No Sessions Yet'
    WHEN ROUND(
      attended_sessions::numeric / NULLIF(sessions_happened, 0) * 100,
      1
    ) >= 90 THEN 'Healthy'
    WHEN ROUND(
      attended_sessions::numeric / NULLIF(sessions_happened, 0) * 100,
      1
    ) >= 75 THEN 'At Risk'
    WHEN ROUND(
      attended_sessions::numeric / NULLIF(sessions_happened, 0) * 100,
      1
    ) < 80 THEN 'Unhealthy'
    ELSE 'Unhealthy'
  END AS consistency_status
FROM child_aggregated
ORDER BY chapter_id, consistency_status
