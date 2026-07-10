{{ config(materialized='table') }}

WITH trimmed AS (
  SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    NULLIF(TRIM("City_Name"), '')               AS city_name,
    NULLIF(TRIM("State"), '')                   AS state,
    NULLIF(TRIM("CO_ID"), '')::integer          AS co_id,
    NULLIF(TRIM("CO_Name"), '')                 AS sheet_co_name,
    NULLIF(TRIM("Chapter_ID"), '')              AS chapter_id,
    NULLIF(TRIM("Chapter_Name"), '')            AS chapter_name,
    NULLIF(TRIM("Worknode_ID"), '')::integer    AS worknode_id,
    NULLIF(TRIM("CHO_ID"), '')::integer         AS cho_id,
    NULLIF(TRIM("CHO_Name"), '')                AS sheet_cho_name,
    NULLIF(TRIM("Engine"), '')                  AS engine,
    NULLIF(TRIM("Chapter_Status"), '')          AS chapter_status,
    NULLIF(TRIM("CHO_Status"), '')              AS cho_status,
    NULLIF(TRIM("Sourcing_Campaign_Code"), '')  AS sourcing_campaign_code,
    NULLIF(TRIM("Campaign_Name"), '')           AS campaign_name,
    NULLIF(TRIM("Fundraiser_ID"), '')           AS fundraiser_id,
    NULLIF(TRIM("Fundraiser_Name"), '')         AS fundraiser_name,
    NULLIF(TRIM("CHO_Allocation_on_PC_"), '')   AS cho_allocation_on_pc,
    NULLIF(TRIM("_Validation_Status"), '')      AS sheet_validation_status,
    NULLIF(TRIM("_Validation_Issues"), '')      AS sheet_validation_issues
  FROM {{ source('google_sheet_staging', 'master_mapping_sheet') }}
),

-- Check 1: Duplicate composite key (chapter_id, cho_id) — both rows flagged
dup_chapter_cho AS (
  SELECT
    _airbyte_raw_id,
    'Duplicate (chapter_id, cho_id) — also in ' || (cnt - 1) || ' other row(s)' AS issue_text
  FROM (
    SELECT
      _airbyte_raw_id,
      COUNT(*) OVER (PARTITION BY chapter_id, cho_id) AS cnt
    FROM trimmed
  ) s
  WHERE cnt > 1
),

-- Check 2: Duplicate fundraiser_id — both rows flagged
dup_fundraiser AS (
  SELECT
    _airbyte_raw_id,
    'Duplicate fundraiser_id ''' || fundraiser_id || ''' — also in ' || (cnt - 1) || ' other row(s)' AS issue_text
  FROM (
    SELECT
      _airbyte_raw_id,
      fundraiser_id,
      COUNT(*) OVER (PARTITION BY fundraiser_id) AS cnt
    FROM trimmed
    WHERE fundraiser_id IS NOT NULL
  ) s
  WHERE cnt > 1
),

-- Check 3: Duplicate sourcing_campaign_code — both rows flagged
dup_campaign_code AS (
  SELECT
    _airbyte_raw_id,
    'Duplicate sourcing_campaign_code ''' || sourcing_campaign_code || ''' — also in ' || (cnt - 1) || ' other row(s)' AS issue_text
  FROM (
    SELECT
      _airbyte_raw_id,
      sourcing_campaign_code,
      COUNT(*) OVER (PARTITION BY sourcing_campaign_code) AS cnt
    FROM trimmed
    WHERE sourcing_campaign_code IS NOT NULL
  ) s
  WHERE cnt > 1
),

-- Check 4a: chapter_id required when Active
active_missing_chapter_id AS (
  SELECT _airbyte_raw_id, 'Missing chapter_id (required for Active)' AS issue_text
  FROM trimmed
  WHERE chapter_status = 'Active' AND chapter_id IS NULL
),

-- Check 4b: cho_id required when Active
active_missing_cho_id AS (
  SELECT _airbyte_raw_id, 'Missing cho_id (required for Active)' AS issue_text
  FROM trimmed
  WHERE chapter_status = 'Active' AND cho_id IS NULL
),

-- Check 4c: worknode_id required when Active
active_missing_worknode_id AS (
  SELECT _airbyte_raw_id, 'Missing worknode_id (required for Active)' AS issue_text
  FROM trimmed
  WHERE chapter_status = 'Active' AND worknode_id IS NULL
),

-- Check 4d: fundraiser_id required when Active
active_missing_fundraiser_id AS (
  SELECT _airbyte_raw_id, 'Missing fundraiser_id (required for Active)' AS issue_text
  FROM trimmed
  WHERE chapter_status = 'Active' AND fundraiser_id IS NULL
),

-- Check 5: engine must be E1 or E2
invalid_engine AS (
  SELECT
    _airbyte_raw_id,
    'Invalid engine ''' || COALESCE(engine, 'NULL') || '''' AS issue_text
  FROM trimmed
  WHERE engine IS NULL OR engine NOT IN ('E1', 'E2')
),

-- Check 6: chapter_status must be Active, Dropped out, or Pending status
invalid_chapter_status AS (
  SELECT
    _airbyte_raw_id,
    'Invalid chapter_status ''' || COALESCE(chapter_status, 'NULL') || '''' AS issue_text
  FROM trimmed
  WHERE chapter_status IS NULL
     OR chapter_status NOT IN ('Active', 'Dropped out', 'Pending status')
),

-- Check 7: fundraiser_id must be numeric if present
fundraiser_not_numeric AS (
  SELECT
    _airbyte_raw_id,
    'fundraiser_id ''' || fundraiser_id || ''' is not numeric' AS issue_text
  FROM trimmed
  WHERE fundraiser_id IS NOT NULL
    AND fundraiser_id ~ '[^0-9]'
),

-- Check 9: cho_id provided but not found in user_data_int
invalid_cho_id AS (
  SELECT t._airbyte_raw_id, 'cho_id ''' || t.cho_id::text || ''' not found in user data' AS issue_text
  FROM trimmed t
  WHERE t.cho_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM {{ ref('user_data_int') }} u WHERE u.user_id::numeric::integer = t.cho_id
    )
),

all_issues AS (
  SELECT _airbyte_raw_id, issue_text FROM dup_chapter_cho
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM dup_fundraiser
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM dup_campaign_code
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM active_missing_chapter_id
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM active_missing_cho_id
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM active_missing_worknode_id
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM active_missing_fundraiser_id
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM invalid_engine
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM invalid_chapter_status
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM fundraiser_not_numeric
  UNION ALL
  SELECT _airbyte_raw_id, issue_text FROM invalid_cho_id
),

issue_agg AS (
  SELECT
    _airbyte_raw_id,
    STRING_AGG(issue_text, '; ' ORDER BY issue_text) AS validation_issues,
    COUNT(*)::integer                                 AS issue_count
  FROM all_issues
  GROUP BY _airbyte_raw_id
)

SELECT
  t.city_name,
  t.state,
  t.co_id,
  t.sheet_co_name,
  co_user.user_display_name                             AS co_name,
  t.chapter_id,
  t.chapter_name,
  t.worknode_id,
  t.cho_id,
  t.sheet_cho_name,
  cho_user.user_display_name                            AS cho_name,
  t.engine,
  t.chapter_status,
  t.cho_status,
  t.sourcing_campaign_code,
  t.campaign_name,
  t.fundraiser_id,
  t.fundraiser_name,
  t.cho_allocation_on_pc,
  t.sheet_validation_status,
  t.sheet_validation_issues,
  CASE
    WHEN t.engine = 'E2' AND t.chapter_id IS NOT NULL THEN partner_chapter.id IS NOT NULL
    ELSE NULL
  END                                                   AS chapter_validation,
  COALESCE(i.issue_count, 0) = 0                        AS validation_status,
  i.validation_issues,
  COALESCE(i.issue_count, 0)                            AS issue_count,
  CURRENT_TIMESTAMP                                     AS validation_run_at,
  t._airbyte_raw_id,
  t._airbyte_extracted_at,
  t._airbyte_meta
FROM trimmed t
LEFT JOIN {{ ref('user_data_int') }} co_user  ON co_user.user_id::numeric::integer = t.co_id
LEFT JOIN {{ ref('user_data_int') }} cho_user ON cho_user.user_id::numeric::integer = t.cho_id
LEFT JOIN {{ ref('partners_int') }} partner_chapter ON t.engine = 'E2' AND t.chapter_id = partner_chapter.id
LEFT JOIN issue_agg i ON t._airbyte_raw_id = i._airbyte_raw_id
