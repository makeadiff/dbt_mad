{{ config(
  materialized='table',
  schema='external_apps'
) }}

-- Combined COPFR (Community Organizer Performance Fundraising Report) data for both E1 and E2 engines
-- Unions fundraising data from both engines with their respective chapter mappings

WITH e1_data AS (
  SELECT
    fundraiser_id,
    fundraiser_name,
    user_id,
    name,
    user_login,
    role,
    donation_amount,
    donation_length,
    total_amount,
    payment_date,
    campaign_name,
    donation_type,
    payment_status,
    chapter_id,
    chapter_name,
    city,
    engine
  FROM {{ ref('e1_copfr_data') }}
),

e2_data AS (
  SELECT
    fundraiser_id,
    fundraiser_name,
    user_id,
    name,
    user_login,
    role,
    donation_amount,
    donation_length,
    total_amount,
    payment_date,
    campaign_name,
    donation_type,
    payment_status,
    chapter_id,
    chapter_name,
    city,
    engine
  FROM {{ ref('e2_copfr_data') }}
)

-- Union both E1 and E2 data
SELECT * FROM e1_data
UNION ALL
SELECT * FROM e2_data
ORDER BY payment_date DESC, user_id
