{{ config(materialized='table') }}

WITH section_base AS (
  SELECT DISTINCT ON (va.slot_class_section_id, va.academic_year)
    va.partner_id,
    va.slot_class_section_id,
    va.academic_year,
    va.day_of_week,
    va.sessions_start_date,
    va.slot_class_sec_end_date,
    cs.school_id
  FROM {{ ref('volunteer_allocation_history_e2_sessions') }} va
  JOIN {{ ref('slot_class_section_int') }} scs
    ON va.slot_class_section_id = scs.slot_class_section_id
  JOIN {{ ref('class_section_int') }} cs
    ON scs.class_section_id = cs.class_section_id
  ORDER BY
    va.slot_class_section_id,
    va.academic_year,
    va.volunteer_id
),

section_with_dates AS (
  SELECT
    sb.*,
    CASE TRIM(sb.day_of_week)
      WHEN 'Sunday' THEN 0
      WHEN 'Monday' THEN 1
      WHEN 'Tuesday' THEN 2
      WHEN 'Wednesday' THEN 3
      WHEN 'Thursday' THEN 4
      WHEN 'Friday' THEN 5
      WHEN 'Saturday' THEN 6
      ELSE NULL
    END AS slot_dow,
    sb.sessions_start_date::date AS effective_start,
    LEAST(
      CURRENT_DATE,
      COALESCE(
        sb.slot_class_sec_end_date::date,
        CASE
          WHEN sb.academic_year = '2025-2026' THEN '2026-04-30'::date
          WHEN sb.academic_year = '2026-2027' THEN '2027-04-30'::date
          ELSE NULL
        END
      )
    ) AS effective_end
  FROM section_base sb
),

section_with_first_session AS (
  SELECT
    *,
    CASE
      WHEN slot_dow IS NOT NULL AND effective_start IS NOT NULL
      THEN (
        effective_start
        + (
          (slot_dow - EXTRACT(DOW FROM effective_start)::int + 7) % 7
        )
      )
      ELSE NULL
    END AS first_session_date
  FROM section_with_dates
),

planned_dates AS (
  SELECT
    swfs.partner_id,
    swfs.slot_class_section_id,
    swfs.academic_year,
    swfs.school_id,
    swfs.first_session_date + (gs.n * 7) AS planned_date
  FROM section_with_first_session swfs
  CROSS JOIN generate_series(0, 52) AS gs(n)
  WHERE swfs.first_session_date IS NOT NULL
    AND swfs.effective_end IS NOT NULL
    AND swfs.first_session_date + (gs.n * 7) <= swfs.effective_end
),

cancelled_dates AS (
  SELECT DISTINCT ON (pd.slot_class_section_id, pd.planned_date)
    pd.slot_class_section_id,
    pd.academic_year,
    pd.planned_date,
    sh.holiday_reason
  FROM planned_dates pd
  INNER JOIN {{ ref('school_holiday_int') }} sh
    ON pd.school_id = sh.school_id
    AND pd.planned_date >= sh.start_date::date
    AND pd.planned_date <= sh.end_date::date
    AND sh.removed = FALSE
  ORDER BY
    pd.slot_class_section_id,
    pd.planned_date,
    sh.holiday_reason
),

cancellations_per_section AS (
  SELECT
    slot_class_section_id,
    academic_year,
    COUNT(*) AS total_cancellations,
    STRING_AGG(DISTINCT holiday_reason, ', ' ORDER BY holiday_reason) AS cancellation_reasons
  FROM cancelled_dates
  GROUP BY
    slot_class_section_id,
    academic_year
)

SELECT
  sb.partner_id,
  sb.slot_class_section_id,
  sb.academic_year,
  COALESCE(cps.total_cancellations, 0) AS total_cancellations,
  cps.cancellation_reasons
FROM section_base sb
LEFT JOIN cancellations_per_section cps
  ON sb.slot_class_section_id = cps.slot_class_section_id
  AND sb.academic_year = cps.academic_year
