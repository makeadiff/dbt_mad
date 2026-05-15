{{ config(materialized='table') }}

SELECT
  city_name,
  state,
  co_id,
  co_name,
  chapter_id,
  chapter_name,
  worknode_id,
  cho_id,
  cho_name,
  engine,
  chapter_status,
  sourcing_campaign_code,
  campaign_name,
  fundraiser_id,
  chapter_validation,
  fundraiser_name
FROM {{ ref('master_mapping_sheet_int') }}
WHERE validation_status = true
