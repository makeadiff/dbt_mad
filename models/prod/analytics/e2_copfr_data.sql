{{ config(
  materialized='table'
) }}

WITH user_data AS (
  SELECT
    user_id,
    user_display_name as name,
    user_role as role,
    user_login,
    center
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

volunteer_fundraising_summary AS (
  SELECT
    volunteer_id,
    first_donation_date,
    total_amount_raised,
    raised_first_500,
    raised_recurring_donation,
    partner_id,
    partner_name,
    partner_city
  FROM {{ ref('volunteer_fundraising_summary') }}
),

e1_e2_co_mapping AS (
  SELECT
    co_id,
    chapter_id,
    chapter_name,
    city_name,
    engine
  FROM {{ ref('e1_e2_co_mapping_int') }}
  WHERE engine = 'E2'
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
)

-- Start with fundraising donations and join with user data
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
  -- Chapter details: prioritize volunteer_fundraising_summary (E2), fallback to e1_e2_co_mapping
  COALESCE(vfs.partner_id::text, e2.chapter_id::text) as chapter_id,
  COALESCE(vfs.partner_name, e2.chapter_name) as chapter_name,
  COALESCE(vfs.partner_city, e2.city_name) as city,
  CASE
    WHEN vfs.volunteer_id IS NOT NULL OR ud.center = 'E2' OR e2.co_id IS NOT NULL THEN 'E2'
    ELSE 'E1'
  END as engine
FROM donations_with_total dw
INNER JOIN user_data ud
  ON dw.fundraiser_id::text = ud.user_id::numeric::integer::text
LEFT JOIN volunteer_fundraising_summary vfs
  ON dw.fundraiser_id::integer = vfs.volunteer_id::numeric::integer
LEFT JOIN e1_e2_co_mapping e2
  ON ud.user_id::numeric::integer = e2.co_id
WHERE vfs.volunteer_id IS NOT NULL
   OR ud.center = 'E2'
   OR e2.co_id IS NOT NULL
