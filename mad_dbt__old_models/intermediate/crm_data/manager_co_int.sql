{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'manager_co') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- Manager to Community Organizer relationship assignments
SELECT
   id::text,
   manager_id::text,
   co_id::text,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated