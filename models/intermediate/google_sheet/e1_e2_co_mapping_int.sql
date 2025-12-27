{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('google_sheet_staging', 'e1_e2_co_mapping') }}
)

-- E1 E2 CO mapping data with validations
-- Filters out rows where required fields (co_id, engine, co_name, city_name, chapter_id, chapter_name) have missing values
SELECT
   co_id::integer,
   engine,
   co_name,
   city_name,
   chapter_id,
   chapter_name,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM source_data
WHERE co_id IS NOT NULL
  AND TRIM(co_id::text) != ''
  AND engine IS NOT NULL
  AND TRIM(engine) != ''
  AND co_name IS NOT NULL
  AND TRIM(co_name) != ''
  AND city_name IS NOT NULL
  AND TRIM(city_name) != ''
  AND chapter_id IS NOT NULL
  AND TRIM(chapter_id::text) != ''
  AND chapter_name IS NOT NULL
  AND TRIM(chapter_name) != ''
