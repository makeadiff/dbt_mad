{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'cities') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- City master data linked to states
SELECT
   id::text,
   state_id::text,
   city_name,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated