{{ config(materialized='table') }}

WITH class_sections_with_slot AS (
  SELECT DISTINCT class_section_id
  FROM {{ ref('slot_class_section_int') }}
  WHERE removed = FALSE
),

school_class_sections AS (
  SELECT
    ay.label AS academic_year,
    p.partner_id1::text AS chapter_id,
    cs.class_section_id
  FROM {{ ref('academic_year_int') }} ay
  JOIN {{ ref('school_academic_year_int') }} say
    ON ay.academic_year_id = say.academic_year_id
    AND say.removed = FALSE
  JOIN {{ ref('school_class_int') }} scl
    ON say.school_academic_year_id = scl.school_academic_year_id
    AND say.school_id = scl.school_id
    AND scl.removed = FALSE
  JOIN {{ ref('class_section_int') }} cs
    ON scl.school_class_id = cs.school_class_id
    AND cs.removed = FALSE
  JOIN {{ ref('partner_int') }} p
    ON say.school_id = p.partner_id1
),

children_by_school_ay AS (
  SELECT
    scs.chapter_id,
    scs.academic_year,
    COUNT(DISTINCT ccs.child_id) AS total_children_in_system,
    COUNT(DISTINCT ccs.child_id) FILTER (
      WHERE cws.class_section_id IS NOT NULL
    ) AS total_children_with_mentor,
    COUNT(DISTINCT ccs.child_id) FILTER (
      WHERE cws.class_section_id IS NULL
    ) AS children_without_mentor,
    COUNT(DISTINCT scs.class_section_id) FILTER (
      WHERE cws.class_section_id IS NULL
    ) AS sections_without_volunteer
  FROM school_class_sections scs
  LEFT JOIN {{ ref('child_class_section_int') }} ccs
    ON scs.class_section_id = ccs.class_section_id
    AND ccs.removed_boolean = FALSE
  LEFT JOIN class_sections_with_slot cws
    ON scs.class_section_id = cws.class_section_id
  GROUP BY
    scs.chapter_id,
    scs.academic_year
),

volunteer_metrics AS (
  SELECT
    chapter_id::text AS chapter_id,
    academic_year,
    COUNT(DISTINCT volunteer_id) AS total_volunteers,
    COUNT(*) FILTER (WHERE consistency_status = 'Healthy') AS volunteers_healthy,
    COUNT(*) FILTER (WHERE consistency_status = 'At Risk') AS volunteers_at_risk,
    COUNT(*) FILTER (WHERE consistency_status = 'Unhealthy') AS volunteers_unhealthy,
    COUNT(*) FILTER (WHERE consistency_status = 'No Sessions Yet') AS volunteers_no_sessions
  FROM {{ ref('fct_volunteer_consistency') }}
  GROUP BY
    chapter_id::text,
    academic_year
),

child_metrics AS (
  SELECT
    chapter_id::text AS chapter_id,
    academic_year,
    COUNT(DISTINCT child_id) AS total_children_in_slots,
    COUNT(*) FILTER (WHERE consistency_status = 'Healthy') AS children_healthy,
    COUNT(*) FILTER (WHERE consistency_status = 'At Risk') AS children_at_risk,
    COUNT(*) FILTER (WHERE consistency_status = 'Unhealthy') AS children_unhealthy,
    COUNT(*) FILTER (WHERE consistency_status = 'No Sessions Yet') AS children_no_sessions
  FROM {{ ref('fct_child_consistency') }}
  GROUP BY
    chapter_id::text,
    academic_year
),

mapping_sheet_deduped AS (
  SELECT DISTINCT ON (chapter_id)
    chapter_id,
    chapter_name,
    city_name,
    co_name,
    cho_name,
    engine,
    chapter_status
  FROM {{ ref('master_mapping_sheet_int') }}
  WHERE chapter_id IS NOT NULL
  ORDER BY
    chapter_id,
    validation_status DESC,
    _airbyte_extracted_at DESC
),

chapter_dimensions AS (
  SELECT
    p.partner_id1::text AS chapter_id,
    COALESCE(mm.chapter_name, p.partner_name) AS chapter_name,
    mm.city_name,
    mm.co_name,
    mm.cho_name,
    mm.engine,
    mm.chapter_status
  FROM {{ ref('partner_int') }} p
  LEFT JOIN mapping_sheet_deduped mm
    ON p.partner_id1::text = mm.chapter_id
),

chapter_academic_years AS (
  SELECT
    mm.chapter_id::text AS chapter_id,
    ay.label AS academic_year
  FROM (
    SELECT DISTINCT ON (chapter_id)
      chapter_id
    FROM {{ ref('master_mapping_sheet_int') }}
    WHERE
      chapter_id IS NOT NULL
      AND chapter_status = 'Active'
      AND engine = 'E2'
    ORDER BY
      chapter_id,
      validation_status DESC,
      _airbyte_extracted_at DESC
  ) mm
  CROSS JOIN {{ ref('academic_year_int') }} ay
)

SELECT
  cay.chapter_id,
  cd.chapter_name,
  cd.city_name,
  cd.co_name,
  cd.cho_name,
  cd.engine,
  cd.chapter_status,
  cay.academic_year,
  ss.total_planned_sessions,
  ss.total_sessions_happened,
  ss.total_original_sessions,
  ss.total_substitute_sessions,
  ss.total_cancellations,
  ss.total_absenteeism,
  ss.pct_sessions_happened,
  ss.pct_original_sessions,
  ss.pct_substitute_sessions,
  ss.pct_cancellations,
  COALESCE(cbs.sections_without_volunteer, 0) AS sections_without_volunteer,
  COALESCE(cbs.children_without_mentor, 0) AS children_without_mentor,
  cbs.total_children_with_mentor,
  vm.total_volunteers,
  vm.volunteers_healthy,
  vm.volunteers_at_risk,
  vm.volunteers_unhealthy,
  vm.volunteers_no_sessions,
  ROUND(
    vm.volunteers_healthy * 100.0 / NULLIF(vm.total_volunteers, 0),
    1
  ) AS pct_volunteers_consistent,
  ROUND(
    vm.volunteers_at_risk * 100.0 / NULLIF(vm.total_volunteers, 0),
    1
  ) AS pct_volunteers_at_risk,
  ROUND(
    vm.volunteers_unhealthy * 100.0 / NULLIF(vm.total_volunteers, 0),
    1
  ) AS pct_volunteers_unhealthy,
  ROUND(
    vm.volunteers_no_sessions * 100.0 / NULLIF(vm.total_volunteers, 0),
    1
  ) AS pct_volunteers_no_sessions,
  cm.total_children_in_slots,
  cm.children_healthy,
  cm.children_at_risk,
  cm.children_unhealthy,
  cm.children_no_sessions,
  ROUND(
    cm.children_healthy * 100.0 / NULLIF(cm.total_children_in_slots, 0),
    1
  ) AS pct_children_consistent,
  ROUND(
    cm.children_at_risk * 100.0 / NULLIF(cm.total_children_in_slots, 0),
    1
  ) AS pct_children_at_risk,
  ROUND(
    cm.children_unhealthy * 100.0 / NULLIF(cm.total_children_in_slots, 0),
    1
  ) AS pct_children_unhealthy,
  ROUND(
    cm.children_no_sessions * 100.0 / NULLIF(cm.total_children_in_slots, 0),
    1
  ) AS pct_children_no_sessions,
  cbs.total_children_in_system
FROM chapter_academic_years cay
LEFT JOIN chapter_dimensions cd
  ON cay.chapter_id = cd.chapter_id
LEFT JOIN {{ ref('fct_sessions_summary') }} ss
  ON cay.chapter_id = ss.partner_id::text
  AND cay.academic_year = ss.academic_year
LEFT JOIN children_by_school_ay cbs
  ON cay.chapter_id = cbs.chapter_id
  AND cay.academic_year = cbs.academic_year
LEFT JOIN volunteer_metrics vm
  ON cay.chapter_id = vm.chapter_id
  AND cay.academic_year = vm.academic_year
LEFT JOIN child_metrics cm
  ON cay.chapter_id = cm.chapter_id
  AND cay.academic_year = cm.academic_year
