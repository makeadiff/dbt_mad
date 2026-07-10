{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'meetings') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- Meeting records between staff and partner contacts
SELECT
   id::text,
   partner_id::text,
   poc_id::text,
   user_id::text,
   meeting_date,
   follow_up_meeting_date,
   follow_up_meeting_scheduled,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated