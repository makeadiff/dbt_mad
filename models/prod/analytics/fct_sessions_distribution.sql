{{ config(materialized='table') }}

SELECT
  partner_id,
  academic_year,
  chapter_name,
  city_name,
  co_name,
  engine,
  chapter_status,
  'Conducted' AS category,
  total_sessions_happened AS value
FROM {{ ref('fct_sessions_summary') }}

UNION ALL

SELECT
  partner_id,
  academic_year,
  chapter_name,
  city_name,
  co_name,
  engine,
  chapter_status,
  'Cancelled' AS category,
  total_cancellations AS value
FROM {{ ref('fct_sessions_summary') }}

UNION ALL

SELECT
  partner_id,
  academic_year,
  chapter_name,
  city_name,
  co_name,
  engine,
  chapter_status,
  'Absenteeism' AS category,
  total_absenteeism AS value
FROM {{ ref('fct_sessions_summary') }}
