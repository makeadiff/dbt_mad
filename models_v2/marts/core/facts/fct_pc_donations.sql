{{ config(materialized='table') }}

select
    donation_key,
    donor_key,
    opportunity_id,
    campaign_name,
    payment_date,
    payment_status,
    donation_type,
    donation_amount,
    tip_amount
from {{ ref('int_pc_fundraising_donations') }}
