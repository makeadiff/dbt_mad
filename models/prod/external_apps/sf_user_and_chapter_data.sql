{{ config(materialized='table') }}

WITH deduped AS (
  -- 1 chapter = 1 fundraiser. When multiple rows share a chapter_id, keep the row
  -- with the lowest fundraiser_id so chapter_id and fundraiser_id are both unique.
  SELECT
    state,
    city_name,
    chapter_id,
    chapter_name,
    co_id,
    co_name,
    cho_id,
    cho_name,
    chapter_status AS status,
    fundraiser_id,
    fundraiser_name,
    NULL::text AS area,
    ROW_NUMBER() OVER (
      PARTITION BY chapter_id
      ORDER BY fundraiser_id ASC
    ) AS rn
  FROM {{ ref('master_mapping_sheet_int') }}
  WHERE chapter_status = 'Active'
    AND chapter_id IS NOT NULL
    AND fundraiser_id IS NOT NULL
)

SELECT
  state,
  city_name,
  chapter_id,
  chapter_name,
  co_id,
  co_name,
  cho_id,
  cho_name,
  status,
  fundraiser_id,
  fundraiser_name,
  area
FROM deduped
WHERE rn = 1
