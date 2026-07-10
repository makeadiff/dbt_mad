{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'mous') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- Memorandum of Understanding records between organization and partners
SELECT
   id::text,
   partner_id::text,
   mou_url,
   mou_sign,
   mou_status,
   mou_start_date,
   mou_end_date,
   mou_sign_date,
   pending_mou_reason,
   confirmed_child_count,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated