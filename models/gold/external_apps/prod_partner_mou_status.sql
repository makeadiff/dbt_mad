{{ config(materialized='table') }}

WITH latest_agreement AS (
  SELECT
    partner_id,
    conversion_stage,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, agreement_id DESC) as rn
  FROM {{ ref('fct_partner_agreements') }}
),

latest_mou AS (
  SELECT
    partner_id,
    mou_id,
    mou_url,
    mou_status,
    mou_start_date,
    mou_end_date,
    mou_sign_date,
    confirmed_child_count,
    ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, mou_id DESC) as rn
  FROM {{ ref('dim_mou') }}
)

SELECT
  p.crm_partner_id::integer AS partner_id,
  p.partner_name,
  p.city,
  p.state,
  p.school_type,
  la.conversion_stage AS latest_conversion_stage,
  (la.conversion_stage = 'converted') AS is_converted,
  lm.mou_id,
  lm.mou_url,
  lm.mou_status,
  lm.mou_start_date,
  lm.mou_end_date,
  lm.mou_sign_date,
  lm.confirmed_child_count
FROM {{ ref('dim_crm_partner') }} p
LEFT JOIN latest_agreement la
  ON p.crm_partner_id = la.partner_id
  AND la.rn = 1
LEFT JOIN latest_mou lm
  ON p.crm_partner_id = lm.partner_id
  AND lm.rn = 1
WHERE p.is_removed = false
