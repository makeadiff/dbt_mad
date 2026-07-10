{{ config(materialized='table') }}

SELECT
  partner_id,
  academic_year,
  chapter_name,
  city_name,
  co_name,
  engine,
  chapter_status,
  TRIM(UNNEST(STRING_TO_ARRAY(cancellation_reasons, '; '))) AS cancellation_reason,
  total_cancellations
FROM {{ ref('fct_sessions_summary') }}
WHERE cancellation_reasons IS NOT NULL
