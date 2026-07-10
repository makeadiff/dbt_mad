{{ config(materialized='table') }}

WITH section_allocation AS (
  SELECT DISTINCT ON (slot_class_section_id, academic_year)
    partner_id,
    slot_class_section_id,
    academic_year,
    day_of_week,
    sessions_start_date,
    slot_class_sec_end_date
  FROM {{ ref('volunteer_allocation_history_e2_sessions') }}
  ORDER BY
    slot_class_section_id,
    academic_year,
    volunteer_id
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
  FROM section_allocation
),

planned_sessions_calc AS (
  SELECT
    *,
    sessions_start_date::date AS effective_start,
    LEAST(
      CURRENT_DATE,
      COALESCE(
        slot_class_sec_end_date::date,
        CASE
          WHEN academic_year = '2025-2026' THEN '2026-04-30'::date
          WHEN academic_year = '2026-2027' THEN '2027-04-30'::date
          ELSE NULL
        END
      )
    ) AS effective_end
  FROM allocation_with_dow
),

planned_sessions_per_section AS (
  SELECT
    partner_id,
    slot_class_section_id,
    academic_year,
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
    END AS planned_sessions
  FROM planned_sessions_calc
),

sessions_happened_per_section AS (
  SELECT
    slot_class_section_id,
    academic_year,
    COUNT(DISTINCT date_of_slot) AS sessions_happened,
    COUNT(DISTINCT date_of_slot) FILTER (WHERE is_substitute = FALSE) AS original_sessions,
    COUNT(DISTINCT date_of_slot) FILTER (WHERE is_substitute = TRUE) AS substitute_sessions
  FROM {{ ref('bot_attendance_actual_slot_date') }}
  GROUP BY
    slot_class_section_id,
    academic_year
),

chapter_dimensions AS (
  SELECT DISTINCT ON (chapter_id)
    chapter_id::text AS partner_id,
    chapter_name,
    city_name,
    co_name,
    engine,
    chapter_status
  FROM {{ ref('master_mapping_sheet_int') }}
  WHERE chapter_id IS NOT NULL
  ORDER BY
    chapter_id,
    validation_status DESC,
    _airbyte_extracted_at DESC
),

-- NOTE: Currently filtered to E2 schools only.
-- TODO: When E1 attendance data is integrated (different source system),
-- remove the engine filter and add 'engine' as a column in the final SELECT
-- so the dashboard can filter by engine (E1/E2) via a dashboard-level filter.
chapter_academic_years AS (
  SELECT
    mm.chapter_id::text AS partner_id,
    ay.label AS academic_year
  FROM (
    SELECT DISTINCT ON (chapter_id)
      chapter_id,
      chapter_status,
      engine
    FROM {{ ref('master_mapping_sheet_int') }}
    WHERE chapter_id IS NOT NULL
    ORDER BY
      chapter_id,
      validation_status DESC,
      _airbyte_extracted_at DESC
  ) mm
  CROSS JOIN {{ ref('academic_year_int') }} ay
  WHERE
    mm.chapter_status = 'Active'
    AND mm.engine = 'E2'
),

section_metrics AS (
  SELECT
    p.partner_id,
    p.slot_class_section_id,
    p.academic_year,
    p.planned_sessions,
    COALESCE(h.sessions_happened, 0) AS sessions_happened,
    COALESCE(h.original_sessions, 0) AS original_sessions,
    COALESCE(h.substitute_sessions, 0) AS substitute_sessions,
    COALESCE(c.total_cancellations, 0) AS total_cancellations,
    c.cancellation_reasons
  FROM planned_sessions_per_section p
  LEFT JOIN sessions_happened_per_section h
    ON p.slot_class_section_id = h.slot_class_section_id
    AND p.academic_year = h.academic_year
  LEFT JOIN {{ ref('fct_cancellations') }} c
    ON p.slot_class_section_id = c.slot_class_section_id
    AND p.academic_year = c.academic_year
),

section_metrics_agg AS (
  SELECT
    sm.partner_id::text AS partner_id,
    sm.academic_year,
    SUM(sm.planned_sessions) AS total_planned_sessions,
    SUM(sm.sessions_happened) AS total_sessions_happened,
    SUM(sm.original_sessions) AS total_original_sessions,
    SUM(sm.substitute_sessions) AS total_substitute_sessions,
    GREATEST(
      SUM(sm.planned_sessions)
        - SUM(sm.sessions_happened)
        - SUM(sm.total_cancellations),
      0
    ) AS total_absenteeism,
    SUM(sm.total_cancellations) AS total_cancellations,
    ROUND(
      SUM(sm.sessions_happened)::numeric / NULLIF(SUM(sm.planned_sessions), 0) * 100,
      1
    ) AS pct_sessions_happened,
    ROUND(
      SUM(sm.original_sessions)::numeric / NULLIF(SUM(sm.sessions_happened), 0) * 100,
      1
    ) AS pct_original_sessions,
    ROUND(
      SUM(sm.substitute_sessions)::numeric / NULLIF(SUM(sm.sessions_happened), 0) * 100,
      1
    ) AS pct_substitute_sessions,
    ROUND(
      SUM(sm.total_cancellations)::numeric / NULLIF(SUM(sm.planned_sessions), 0) * 100,
      1
    ) AS pct_cancellations,
    STRING_AGG(DISTINCT sm.cancellation_reasons, '; ' ORDER BY sm.cancellation_reasons)
      AS cancellation_reasons
  FROM section_metrics sm
  GROUP BY
    sm.partner_id::text,
    sm.academic_year
)

SELECT
  cay.partner_id,
  cd.chapter_name,
  cd.city_name,
  cd.co_name,
  cd.engine,
  cd.chapter_status,
  cay.academic_year,
  sma.total_planned_sessions,
  sma.total_sessions_happened,
  sma.total_original_sessions,
  sma.total_substitute_sessions,
  sma.total_absenteeism,
  sma.total_cancellations,
  sma.pct_sessions_happened,
  sma.pct_original_sessions,
  sma.pct_substitute_sessions,
  sma.pct_cancellations,
  sma.cancellation_reasons
FROM chapter_academic_years cay
LEFT JOIN chapter_dimensions cd
  ON cay.partner_id = cd.partner_id
LEFT JOIN section_metrics_agg sma
  ON cay.partner_id = sma.partner_id
  AND cay.academic_year = sma.academic_year
