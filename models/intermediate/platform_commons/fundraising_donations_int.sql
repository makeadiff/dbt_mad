{{ config(materialized='table') }}

SELECT
    "donorName" AS donor_name,
    "tipAmount" AS tip_amount,
    "donorEmail" AS donor_email,
    "donorMobile" AS donor_mobile,

    CASE
        WHEN "paymentDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("paymentDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS payment_date,

    "campaignName" AS campaign_name,
    "donationType" AS donation_type,
    "fundraiserId" AS fundraiser_id,
    "donationAmount" AS donation_amount,
    "donationLength" AS donation_length,
    "fundraiserName" AS fundraiser_name,
    "payment_status" AS payment_status,
    "totalAmountPaid" AS total_amount_paid,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    "donor_campaign_code" AS donor_campaign_code,
    "FundRaiseProgramName" AS fund_raise_program_name,
    "payment_campaign_code" AS payment_campaign_code,
    "gateway_subscription_id" AS gateway_subscription_id,
    "opportunityId" AS opportunity_id,
    "donationId" AS donation_id,
    "campaignId" AS campaign_id

FROM {{ source('source_platform_commons', 'fundraising_donations_int') }}
