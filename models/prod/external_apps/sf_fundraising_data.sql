{{ config(materialized='view') }}

SELECT
  CASE
    WHEN "paymentDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
    THEN TO_TIMESTAMP("paymentDate", 'YYYY-MM-DD"T"HH24:MI:SS')
    ELSE NULL
  END AS payment_date,
  "donationType" AS donation_type,
  "opportunityId" AS fundraiser_id,
  "donationAmount" AS donation_amount,
  "donationId" AS donation_id,
  "donationLength" AS donation_length,
  "payment_status" AS payment_status,
  "fundraiserId" AS funds_raised_by_user_id,
  "fundraiserName" AS funds_raised_by_user_name,
  "gateway_subscription_id" AS gateway_subscription_id,
  NULL::text AS donor_id
FROM {{ source('source_platform_commons', 'fundraising_donations_int') }}
ORDER BY payment_date DESC NULLS LAST
