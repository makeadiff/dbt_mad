{{ config(materialized='table') }}

WITH attendance_agg AS (
  SELECT
    volunteer_id,
    slot_class_section_id,
    academic_year,
    COUNT(DISTINCT date_of_slot) AS attended_sessions,
    COUNT(DISTINCT date_of_slot) FILTER (WHERE is_substitute = FALSE) AS original_sessions,
    COUNT(DISTINCT date_of_slot) FILTER (WHERE is_substitute = TRUE) AS substitute_sessions
  FROM {{ ref('bot_attendance_actual_slot_date') }}
  GROUP BY
    volunteer_id,
    slot_class_section_id,
    academic_year
),

allocation_with_dow AS (
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
  FROM {{ ref('volunteer_allocation_history_e2_sessions') }}
),

planned_sessions_calc AS (
  SELECT
    *,
    GREATEST(
      slot_class_sec_start_date::date,
      sessions_start_date::date
    ) AS effective_start,
    CASE
      WHEN is_active = TRUE THEN LEAST(
        CURRENT_DATE,
        COALESCE(
          slot_class_sec_end_date::date,
          CASE
            WHEN academic_year = '2025-2026' THEN '2026-04-30'::date
            WHEN academic_year = '2026-2027' THEN '2027-04-30'::date
            ELSE NULL
          END
        )
      )
      ELSE COALESCE(slot_class_sec_end_date::date, CURRENT_DATE)
    END AS effective_end
  FROM allocation_with_dow
),

planned_sessions_raw AS (
  SELECT
    *,
    CASE
      WHEN slot_dow IS NOT NULL
        AND effective_start IS NOT NULL
        AND effective_end IS NOT NULL
      THEN GREATEST(
        FLOOR(
          (
            effective_end
            - (
              effective_start
              + (
                (slot_dow - EXTRACT(DOW FROM effective_start)::int + 7) % 7
              )
            )
          ) / 7.0
        )::int + 1,
        0
      )
      ELSE 0
    END AS planned_sessions_raw
  FROM planned_sessions_calc
),

planned_sessions AS (
  SELECT
    psr.*,
    GREATEST(
      psr.planned_sessions_raw - COALESCE(c.total_cancellations, 0),
      0
    ) AS planned_sessions
  FROM planned_sessions_raw psr
  LEFT JOIN {{ ref('fct_cancellations') }} c
    ON psr.slot_class_section_id = c.slot_class_section_id
    AND psr.academic_year = c.academic_year
),

joined AS (
  SELECT
    ps.volunteer_id,
    ps.user_display_name AS volunteer_name,
    ps.partner_id AS chapter_id,
    ps.partner_name AS chapter_name,
    ps.academic_year,
    ps.is_active,
    ps.planned_sessions,
    COALESCE(aa.attended_sessions, 0) AS attended_sessions,
    COALESCE(aa.original_sessions, 0) AS original_sessions,
    COALESCE(aa.substitute_sessions, 0) AS substitute_sessions
  FROM planned_sessions ps
  LEFT JOIN attendance_agg aa
    ON ps.volunteer_id = aa.volunteer_id
    AND ps.slot_class_section_id = aa.slot_class_section_id
    AND ps.academic_year = aa.academic_year
),

volunteer_aggregated AS (
  SELECT
    volunteer_id,
    volunteer_name,
    chapter_id,
    chapter_name,
    academic_year,
    SUM(planned_sessions) AS planned_sessions,
    SUM(attended_sessions) AS attended_sessions,
    SUM(original_sessions) AS original_sessions,
    SUM(substitute_sessions) AS substitute_sessions,
    BOOL_OR(is_active) AS is_active
  FROM joined
  GROUP BY
    volunteer_id,
    volunteer_name,
    chapter_id,
    chapter_name,
    academic_year
)

SELECT
  volunteer_id,
  volunteer_name,
  chapter_id,
  chapter_name,
  academic_year,
  is_active,
  planned_sessions,
  attended_sessions,
  original_sessions,
  substitute_sessions,
  -- TODO: update multiplier when actual session duration per slot is available.
  original_sessions * 2 AS hours_contributed,
  CASE
    WHEN planned_sessions = 0 THEN NULL
    ELSE ROUND(
      original_sessions::numeric / NULLIF(planned_sessions, 0) * 100,
      1
    )
  END AS attendance_pct,
  CASE
    WHEN planned_sessions = 0 THEN 'No Sessions Yet'
    WHEN ROUND(
      original_sessions::numeric / NULLIF(planned_sessions, 0) * 100,
      1
    ) >= 90 THEN 'Healthy'
    WHEN ROUND(
      original_sessions::numeric / NULLIF(planned_sessions, 0) * 100,
      1
    ) >= 75 THEN 'At Risk'
    ELSE 'Unhealthy'
  END AS consistency_status
FROM volunteer_aggregated
ORDER BY chapter_id, consistency_status
