{{ config(materialized='table') }}

-- Volunteer allocation history at volunteer–slot grain (no child expansion).
-- Based on prod analytics volunteer_allocation_history; child/class/subject joins removed.

WITH scs_deduped AS (
  -- Airbyte CDC can write multiple rows for the same slot_class_section_id.
  -- Keep one row per id, preferring active (removed=FALSE) over removed, then latest modified_date.
  SELECT DISTINCT ON (slot_class_section_id)
    *
  FROM {{ ref('slot_class_section_int') }}
  ORDER BY slot_class_section_id, removed ASC, modified_date DESC
),

scsv_deduped AS (
  -- Airbyte CDC can write duplicate removed=TRUE rows for the same volunteer+section.
  -- Deduplicate on (slot_class_section_id, volunteer_id, removed, created_date) so that
  -- legitimate reassignments (one removed + one active row) are preserved as two rows.
  SELECT DISTINCT ON (slot_class_section_id, volunteer_id, removed, created_date::date)
    *
  FROM {{ ref('slot_class_section_volunteer_int') }}
  ORDER BY slot_class_section_id, volunteer_id, removed, created_date::date, modified_date DESC
),

scsv_with_next AS (
  -- modified_date is unreliable as a removal timestamp because schema changes (adding is_active
  -- column) and bulk archive operations update it on already-removed rows. When a volunteer was
  -- re-assigned to the same slot_class_section, the next assignment's created_date is a tighter
  -- upper bound on when the previous assignment actually ended. We use LEAST() to take whichever
  -- is earlier: the next assignment start or the recorded modified_date.
  SELECT
    *,
    LEAD(created_date) OVER (
      PARTITION BY slot_class_section_id, volunteer_id
      ORDER BY created_date
    ) AS next_assignment_start
  FROM scsv_deduped
)

SELECT
  scsv.volunteer_id,
  scsv.slot_class_section_id,
  scsv.created_date AS slot_class_sec_start_date,
  CASE
    WHEN scs.removed = TRUE THEN scs.modified_date
    WHEN scsv.removed = TRUE OR scsv.is_active = FALSE THEN
      -- removed=TRUE: volunteer formally removed from slot.
      -- is_active=FALSE with removed=FALSE: volunteer deactivated without formal removal
      -- (can happen via bulk ops or app-level deactivation).
      -- In both cases, cap modified_date with next_assignment_start to guard against
      -- modified_date inflation from schema backfills or bulk archive operations.
      CASE
        WHEN scsv.next_assignment_start IS NOT NULL
          THEN LEAST(scsv.next_assignment_start, scsv.modified_date)
        ELSE scsv.modified_date
      END
    ELSE NULL
  END AS slot_class_sec_end_date,
  CASE
    WHEN scs.removed = FALSE AND scsv.removed = FALSE AND COALESCE(scsv.is_active, TRUE) = TRUE
    THEN TRUE
    ELSE FALSE
  END AS is_active,
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
FROM scsv_with_next scsv
JOIN scs_deduped scs
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
  cs.removed = FALSE
  AND p.removed = FALSE
