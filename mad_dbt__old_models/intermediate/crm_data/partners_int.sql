{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'partners') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- Partner organizations and schools in the network
SELECT
   id::text,
   partner_name,
   city_id::text,
   state_id::text,
   pincode,
   removed,
   interested,
   lead_source,
   school_type,
   partner_affiliation_type,
   address_line_1,
   address_line_2,
   total_child_count,
   low_income_resource,
   classes,
   created_by::text,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated
