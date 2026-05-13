{{ config(materialized='table') }}

SELECT
  -- Partner core
  p.id::integer AS partner_id,
  p.partner_name,
  p.address_line_1,
  p.address_line_2,
  p.city_id::integer,
  p.state_id::integer,
  cty.city_name AS city,
  p.pincode::integer AS pincode,
  p.lead_source,
  p.school_type,
  p.partner_affiliation_type,
  p.total_child_count,
  p.low_income_resource,
  p.created_by::numeric::integer AS created_by,
  p.removed AS crm_partner_removed,

  -- Latest CO
  latest_co.co_id::numeric::integer,
  -- Latest CO name
  latest_co_name.co_name,

  -- Latest POC (via poc_partners_int → pocs_int)
  latest_poc.poc_name,
  latest_poc.poc_contact,
  latest_poc.poc_email,
  latest_poc.poc_designation,

  -- Classes as array of text (handle JSON array or comma-separated string)
  CASE
    WHEN p.classes::text LIKE '[%' THEN (
      SELECT array_agg(elem) FROM jsonb_array_elements_text(p.classes::jsonb) elem
    )
    WHEN p.classes IS NOT NULL AND p.classes::text <> '' THEN string_to_array(p.classes::text, ',')
    ELSE NULL
  END AS classes,

  -- Latest meeting date (your “date of first contact”)
  latest_meeting.meeting_date AS date_of_first_contact,

  -- State name
  st.state_name AS state,

  -- Latest active MOU
  latest_mou.mou_url,
  latest_mou.mou_start_date,
  latest_mou.mou_end_date,
  latest_mou.mou_sign_date,

  -- Confirmed child count from latest active MOU
  latest_mou.confirmed_child_count,

  -- Latest conversion stage
  latest_pa.conversion_stage AS latest_conversion_stage,

  -- Converted flag based on latest conversion_stage
  CASE
    WHEN latest_pa.conversion_stage = 'converted' THEN TRUE
    ELSE FALSE
  END AS converted,

  -- Partner created/updated dates (converted to IST / Asia/Kolkata)
  -- `AT TIME ZONE 'Asia/Kolkata'` converts a timestamptz to local time (timestamp without time zone)
  (p.created_at AT TIME ZONE 'Asia/Kolkata') AS partner_created_date,
  (GREATEST(
    p.updated_at,
    latest_co.updated_at,
    latest_co_name.updated_at,
    latest_poc.updated_at,
    latest_meeting.updated_at,
    latest_mou.updated_at,
    latest_pa.updated_at
  ) AT TIME ZONE 'Asia/Kolkata') AS partner_updated_date

FROM {{ ref('partners_int') }} p

-- City name
LEFT JOIN {{ ref('cities_int') }} cty
  ON p.city_id::text = cty.id

-- State name
LEFT JOIN {{ ref('states_int') }} st
  ON p.state_id::text = st.id

-- Latest partner CO row
LEFT JOIN LATERAL (
  SELECT pci.co_id, pci.updated_at
  FROM {{ ref('partner_cos_int') }} pci
  WHERE pci.partner_id = p.id
  ORDER BY pci.created_at DESC NULLS LAST
  LIMIT 1
) latest_co ON TRUE

-- Latest CO name from user_data_int
LEFT JOIN LATERAL (
  SELECT ud.user_display_name AS co_name, ud.user_updated_datetime AS updated_at
  FROM {{ ref('user_data_int') }} ud
  WHERE ud.user_id = latest_co.co_id
  LIMIT 1
) latest_co_name ON TRUE

-- Latest POC row: get latest poc_partner, then join poc table
LEFT JOIN LATERAL (
  SELECT
    poc.poc_name        AS poc_name,
    poc.poc_contact     AS poc_contact,
    poc.poc_email       AS poc_email,
    poc.poc_designation AS poc_designation,
    GREATEST(pp.updated_at, poc.updated_at) AS updated_at
  FROM {{ ref('poc_partners_int') }} pp
  JOIN {{ ref('pocs_int') }} poc
    ON poc.id = pp.poc_id
  WHERE pp.partner_id = p.id
  ORDER BY pp.created_at DESC NULLS LAST
  LIMIT 1
) latest_poc ON TRUE

-- Latest meeting row (for contact date)
LEFT JOIN LATERAL (
  SELECT m.meeting_date, m.updated_at
  FROM {{ ref('meetings_int') }} m
  WHERE m.partner_id = p.id
  ORDER BY m.meeting_date DESC NULLS LAST
  LIMIT 1
) latest_meeting ON TRUE

-- Latest active MOU
LEFT JOIN LATERAL (
  SELECT
    mo.mou_url,
    mo.mou_start_date,
    mo.mou_end_date,
    mo.mou_sign_date,
    mo.confirmed_child_count,
    mo.updated_at
  FROM {{ ref('mous_int') }} mo
  WHERE mo.partner_id = p.id
    AND mo.mou_status = 'active'
  ORDER BY mo.created_at DESC NULLS LAST
  LIMIT 1
) latest_mou ON TRUE

-- Latest partner_agreement row
LEFT JOIN LATERAL (
  SELECT pa.conversion_stage, pa.updated_at
  FROM {{ ref('partner_agreements_int') }} pa
  WHERE pa.partner_id = p.id
  ORDER BY pa.created_at DESC NULLS LAST
  LIMIT 1
) latest_pa ON TRUE