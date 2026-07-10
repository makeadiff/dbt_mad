{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'states') }}
)

-- State master data for geographic organization
SELECT
   id::text,
   state_name,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM source_data