{{ config(materialized='table') }}

SELECT
    CASE
        WHEN payment_date::text ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(payment_date::text, 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE payment_date::timestamptz
    END AS payment_date,
    donation_type,
    opportunity_id AS fundraiser_id,
    donation_amount,
    donation_id,
    donation_length,
    payment_status,
    fundraiser_id AS funds_raised_by_user_id,
    fundraiser_name AS funds_raised_by_user_name,
    gateway_subscription_id,
    NULL::text AS donor_id
FROM {{ ref('int_pc_fundraising_donations') }}
ORDER BY payment_date DESC NULLS LAST
