{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('crm_data', 'partner_agreements') }}
),

deduplicated AS (
  {{ dbt_utils.deduplicate(
      relation='source_data',
      partition_by='id',
      order_by='"updatedAt" desc',
     )
  }}
)

-- Partner agreement pipeline and conversion tracking
SELECT
   id::text,
   partner_id::text,
   current_status,
   conversion_stage,
   specific_doc_name,
   specific_doc_required,
   agreement_drop_date,
   non_conversion_reason,
   potential_child_count,
   expected_conversion_day,
   removed,
   "createdAt" AS created_at,
   "updatedAt" AS updated_at,
   _airbyte_raw_id,
   _airbyte_extracted_at,
   _airbyte_meta
FROM deduplicated