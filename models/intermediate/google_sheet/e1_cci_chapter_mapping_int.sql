{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('google_sheet_staging', 'e1_cci_chapter_mapping') }}
),

-- Get all column names dynamically and filter out rows with any null or empty values
filtered_data AS (
  SELECT *
  FROM source_data
  WHERE 1=1
    {% for column in adapter.get_columns_in_relation(source('google_sheet_staging', 'e1_cci_chapter_mapping')) %}
      {% if column.name not in ['_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta'] %}
        AND {{ column.name }} IS NOT NULL
        AND TRIM({{ column.name }}::text) != ''
      {% endif %}
    {% endfor %}
)

-- E1 CCI chapter mapping data with null and empty value checks
-- Filters out rows where any column has null or empty values
SELECT *
FROM filtered_data
