{{ config(materialized='table') }}

with donor as (
    select * from {{ ref('stg_pc_opportunity_donor') }}
),

payment as (
    select * from {{ ref('stg_pc_opportunity_donor_payment') }}
),

-- Join to get actual monetary values instead of IDs
tip as (
    select * from {{ ref('stg_pc_tip') }}
),

final_amount as (
    select * from {{ ref('stg_pc_final_amount') }}
),

fundraiser as (
    select
        o.opportunity_id,
        o.created_by_user as fundraiser_id,
        trim(concat_ws(' ', nullif(u.first_name, ''), nullif(u.last_name, ''))) as fundraiser_name,
        o.opportunity_name as fund_raise_program_name
    from {{ ref('stg_pc_opportunity') }} o
    left join {{ ref('stg_pc_user') }} u on o.created_by_user = u.user_id
    where o.opportunity_type_code = 'OPPORTUNITY_TYPE.FUNDRAISING'
)

select
    {{ dbt_utils.generate_surrogate_key(['p.donor_payment_id']) }} as donation_key,
    {{ dbt_utils.generate_surrogate_key(['d.opportunity_donor_id']) }} as donor_key,

    d.donor_name as donor_name,
    d.donor_email as donor_email,
    d.donor_mobile as donor_mobile,
    d.campaign as campaign_name,
    p.actual_payment_date as payment_date,
    {{ clean_prefix('p.payment_status') }} as payment_status,
    {{ clean_prefix('p.payment_type') }} as donation_type,
    fa.amount_value as donation_amount,
    t.tip_value as tip_amount,
    d.opportunity_id as opportunity_id,
    p.donor_payment_id as donation_id,
    p.campaign as payment_campaign,
    d.campaign as donor_campaign_code,
    p.gateway_subscription_id,
    p.total_count as donation_length,
    fr.fundraiser_id,
    fr.fundraiser_name,
    fr.fund_raise_program_name,
    d.opportunity_donor_id as donor_id,
    p.modified_datetime as user_updated_date_time
from payment p
left join donor d on p.donor_id = d.opportunity_donor_id
left join tip t on p.tip_id = t.tip_id
left join final_amount fa on p.final_amount_id = fa.final_amount_id
left join fundraiser fr on d.opportunity_id = fr.opportunity_id
