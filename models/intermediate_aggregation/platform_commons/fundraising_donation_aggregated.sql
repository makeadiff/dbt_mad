{{ config(materialized='table') }}

with base as (
    select
        donor_name,
        tip_amount,
        donor_email,
        donor_mobile,
        payment_date,
        campaign_name,
        donation_type,
        fundraiser_id,
        donation_amount,
        donation_length,
        fundraiser_name,
        payment_status,
        total_amount_paid,
        payment_campaign,
        user_updated_date_time,
        donor_campaign_code,
        fund_raise_program_name,
        payment_campaign_code,
        gateway_subscription_id,
        opportunity_id,
        donation_id,
        campaign_id
            from {{ ref('fundraising_donations_int') }}
            where upper(payment_status) = 'PAID'
                and payment_date::date >= '2025-10-01'
),
with_key as (
    select
        *,
        coalesce(
            nullif(trim(gateway_subscription_id), ''),
            cast(donation_id as {{ dbt.type_string() }})
        ) as _gw_group_key
    from base
)
select distinct on (_gw_group_key)
    donor_name,
    tip_amount,
    donor_email,
    donor_mobile,
    payment_date,
    campaign_name,
    donation_type,
    fundraiser_id,
    donation_amount,
    donation_length,
    fundraiser_name,
    payment_status,
    total_amount_paid,
    payment_campaign,
    user_updated_date_time,
    donor_campaign_code,
    fund_raise_program_name,
    payment_campaign_code,
    gateway_subscription_id,
    opportunity_id,
    donation_id,
    campaign_id
from with_key
order by
    _gw_group_key,
    payment_date desc nulls last,
    user_updated_date_time desc nulls last,
    donation_id desc
