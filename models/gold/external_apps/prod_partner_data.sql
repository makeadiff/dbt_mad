{{ config(materialized='table') }}

-- Partner Data: Comprehensive partner/school master data view (CRM E2 partners only)
-- Optimized: uses dim_crm_partner (already has city/state joined) + LATERAL joins for latest records

-- TODO: Integrate Platform Commons (PC) data for Partner/School Master Data profiles
SELECT
  p.crm_partner_id::integer AS partner_id,
  p.partner_name,
  p.address_line_1,
  p.address_line_2,
  p.city_id,
  p.state_id,
  p.city,
  p.pincode::integer AS pincode,
  p.lead_source,
  p.school_type,
  p.partner_affiliation_type,
  p.total_child_count,
  p.low_income_resource,
  p.created_by::numeric::integer AS created_by,
  p.is_removed AS crm_partner_removed,

  -- Latest CO
  latest_co.co_id,
  latest_co_user.co_name AS co_name,

  -- Latest POC
  latest_poc.poc_name,
  latest_poc.poc_contact,
  latest_poc.poc_email,
  latest_poc.poc_designation,

  -- Classes
  CASE
    WHEN p.classes::text LIKE '[%' THEN (
      SELECT array_agg(elem) FROM jsonb_array_elements_text(p.classes::jsonb) elem
    )
    WHEN p.classes IS NOT NULL AND p.classes::text <> '' THEN string_to_array(p.classes::text, ',')
    ELSE NULL
  END AS classes,

  -- Latest meeting date
  -- TODO: Integrate Platform Commons (PC) data for Meeting/Touchpoint tracking
  latest_meeting.meeting_date AS date_of_first_contact,

  -- State name
  p.state AS state,

  -- Latest active MOU
  -- TODO: Integrate Platform Commons (PC) data for MOU and Legal Agreement tracking
  latest_mou.mou_url,
  latest_mou.mou_start_date,
  latest_mou.mou_end_date,
  latest_mou.mou_sign_date,
  latest_mou.confirmed_child_count,

  -- Conversion info
  -- TODO: Integrate Platform Commons (PC) data for Partner Conversion Pipeline
  latest_pa.conversion_stage AS latest_conversion_stage,
  CASE
    WHEN latest_pa.conversion_stage = 'converted' THEN TRUE
    ELSE FALSE
  END AS converted,

  -- Partner created/updated dates (converted to IST / Asia/Kolkata)
  (p.created_at AT TIME ZONE 'Asia/Kolkata') AS partner_created_date,
  (GREATEST(
    p.updated_at,
    latest_co.updated_at,
    latest_co_user.updated_at,
    latest_poc.updated_at,
    latest_meeting.updated_at,
    latest_mou.updated_at,
    latest_pa.updated_at
  ) AT TIME ZONE 'Asia/Kolkata') AS partner_updated_date

FROM {{ ref('dim_crm_partner') }} p

LEFT JOIN LATERAL (
  SELECT co_id, updated_at FROM {{ ref('int_crm__partner_cos') }}
  WHERE partner_id = p.crm_partner_id
  ORDER BY created_at DESC NULLS LAST LIMIT 1
) latest_co ON TRUE

LEFT JOIN LATERAL (
  SELECT "UserDisplayName" AS co_name, "UserUpdatedDateTime" AS updated_at
  FROM {{ ref('int_pc_user_data') }}
  WHERE "UserId"::numeric = latest_co.co_id::numeric
  LIMIT 1
) latest_co_user ON TRUE

LEFT JOIN LATERAL (
  SELECT
    poc.poc_name, poc.poc_contact, poc.poc_email, poc.poc_designation,
    GREATEST(pp.updated_at, poc.updated_at) AS updated_at
  FROM {{ ref('int_crm__poc_partners') }} pp
  JOIN {{ ref('dim_poc') }} poc ON poc.poc_id = pp.poc_id
  WHERE pp.partner_id = p.crm_partner_id
  ORDER BY pp.created_at DESC NULLS LAST LIMIT 1
) latest_poc ON TRUE

LEFT JOIN LATERAL (
  SELECT meeting_date, updated_at FROM {{ ref('fct_meetings') }}
  WHERE partner_id = p.crm_partner_id
  ORDER BY meeting_date DESC NULLS LAST LIMIT 1
) latest_meeting ON TRUE

LEFT JOIN LATERAL (
  SELECT mou_url, mou_start_date, mou_end_date, mou_sign_date, confirmed_child_count, updated_at
  FROM {{ ref('dim_mou') }}
  WHERE partner_id = p.crm_partner_id AND mou_status = 'active'
  ORDER BY created_at DESC NULLS LAST LIMIT 1
) latest_mou ON TRUE

LEFT JOIN LATERAL (
  SELECT conversion_stage, updated_at FROM {{ ref('fct_partner_agreements') }}
  WHERE partner_id = p.crm_partner_id
  ORDER BY created_at DESC NULLS LAST LIMIT 1
) latest_pa ON TRUE

WHERE EXISTS (
  SELECT 1
  FROM {{ ref('int_google_sheet__chapter_mapping') }} cm
  WHERE cm.engine = 'E2'
    AND cm.chapter_validation = true
    AND cm.chapter_id ~ '^[0-9]+$'
    AND cm.chapter_id::integer = p.crm_partner_id
)
