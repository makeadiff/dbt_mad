{{ config(materialized='table') }}

with donor as (
    select * from {{ ref('stg_pc_opportunity_donor') }}
),
payment as (
    select * from {{ ref('stg_pc_opportunity_donor_payment') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['p.donor_payment_id']) }} as donation_key,
    {{ dbt_utils.generate_surrogate_key(['d.opportunity_donor_id']) }} as donor_key,

    d.donor_name as donor_name,
    d.donor_email as donor_email,
    d.donor_mobile as donor_mobile,
    d.campaign as campaign_name,
    p.actual_payment_date as payment_date,
    p.payment_status as payment_status,
    p.payment_type as donation_type,
    p.final_amount_id as donation_amount, -- This requires a join to amount table in future
    p.tip_id as tip_amount, -- Requires join to amount table
    d.opportunity_id as opportunity_id
from donor d
left join payment p on d.opportunity_donor_id = p.donor_id
