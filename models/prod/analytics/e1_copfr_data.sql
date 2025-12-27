{{ config(
  materialized='table'
) }}

WITH user_data AS (
  SELECT
    user_id,
    user_display_name as name,
    user_role as role,
    user_login,
    center,
    city
  FROM {{ ref('user_data_int') }}
),

fundraising_donations AS (
  SELECT
    fundraiser_id,
    fundraiser_name,
    donation_amount,
    donation_length,
    payment_date,
    campaign_name,
    donation_type,
    payment_status
  FROM {{ ref('fundraising_donations') }}
),

-- E2 fundraiser IDs to exclude
e2_fundraiser_ids AS (
  SELECT DISTINCT fundraiser_id
  FROM {{ ref('e2_copfr_data') }}
),

e1_cci_chapter_mapping AS (
  SELECT *
  FROM {{ ref('e1_cci_chapter_mapping_int') }}
),

e1_e2_co_mapping AS (
  SELECT
    co_id,
    chapter_id,
    chapter_name,
    city_name,
    engine
  FROM {{ ref('e1_e2_co_mapping_int') }}
  WHERE engine = 'E1'
),

-- Calculate total amount for each donation
donations_with_total AS (
  SELECT
    fundraiser_id,
    fundraiser_name,
    donation_amount,
    donation_length,
    payment_date,
    campaign_name,
    donation_type,
    payment_status,
    CASE
      WHEN donation_length IS NOT NULL
        AND donation_length ~ '^[0-9]+(\.[0-9]+)?$'
      THEN donation_amount * donation_length::numeric
      ELSE donation_amount
    END as total_amount
  FROM fundraising_donations
  WHERE fundraiser_id::text NOT IN (SELECT fundraiser_id::text FROM e2_fundraiser_ids)
)

-- Start with E1 fundraising donations and join with user data
SELECT
  dw.fundraiser_id::integer as fundraiser_id,
  dw.fundraiser_name,
  ud.user_id::numeric::integer as user_id,
  ud.name,
  ud.user_login,
  ud.role,
  dw.donation_amount,
  dw.donation_length,
  dw.total_amount,
  dw.payment_date,
  dw.campaign_name,
  dw.donation_type,
  dw.payment_status,
  -- Chapter details: try e1_cci_chapter_mapping first (by partial center match), fallback to e1_e2_co_mapping, Wingman=Aftercare, final fallback to user center
  COALESCE(cci.chapter_id, e1.chapter_id) as chapter_id,
  CASE
    WHEN cci.chapter_name IS NOT NULL THEN cci.chapter_name
    WHEN e1.chapter_name IS NOT NULL THEN e1.chapter_name
    WHEN ud.role = 'Wingman' AND ud.city IS NOT NULL AND TRIM(ud.city) != '' THEN 'Aftercare'
    ELSE TRIM(ud.center)
  END as chapter_name,
  COALESCE(cci.city_name, e1.city_name) as city,
  'E1' as engine
FROM donations_with_total dw
INNER JOIN user_data ud
  ON dw.fundraiser_id::text = ud.user_id::numeric::integer::text
LEFT JOIN e1_cci_chapter_mapping cci
  ON cci.cci_name ILIKE '%' || TRIM(ud.center) || '%'
  AND ud.center IS NOT NULL
  AND TRIM(ud.center) != ''
LEFT JOIN e1_e2_co_mapping e1
  ON ud.user_id::numeric::integer = e1.co_id
