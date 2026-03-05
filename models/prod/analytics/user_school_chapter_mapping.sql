{{ config(
  materialized='table'
) }}

-- Step 1: Get all active partners (schools) where removed = FALSE
with active_partners as (
  select 
    id as school_id,
    partner_name as school_name
  from {{ ref('partners_int') }}
  where removed = 'FALSE'
),

-- Step 2: Get latest partner agreements for each partner
latest_partner_agreements as (
  select 
    partner_id,
    conversion_stage,
    updated_at
  from (
    select 
      partner_id,
      conversion_stage,
      updated_at,
      row_number() over (partition by partner_id order by updated_at desc) as rn
    from {{ ref('partner_agreements_int') }}
    where removed = 'FALSE'
  ) ranked
  where rn = 1
),

-- Step 3: Get latest CO assignment per partner
latest_partner_cos as (
  select
    partner_id,
    co_id
  from (
    select
      partner_id,
      co_id,
      row_number() over (partition by partner_id order by updated_at desc) as rn
    from {{ ref('partner_cos_int') }}
  ) ranked
  where rn = 1
),

-- Step 4: Left join with latest partner_cos to get CO assignments
partners_with_cos as (
  select
    ap.school_id,
    ap.school_name,
    pco.co_id
  from active_partners ap
  left join latest_partner_cos pco
    on ap.school_id::int = pco.partner_id::int
),

-- Step 5: Filter for only converted partners
converted_partners_with_cos as (
  select 
    pwc.school_id,
    pwc.school_name,
    pwc.co_id
  from partners_with_cos pwc
  inner join latest_partner_agreements lpa
    on pwc.school_id::int = lpa.partner_id::int
  where lpa.conversion_stage = 'converted'
)

-- Final output: Left join with user_data_int to get user names and email
select 
  cpwc.school_id,
  cpwc.school_name,
  cpwc.co_id as user_id,
  u.user_display_name as user_name,
  u.email as user_email
from converted_partners_with_cos cpwc
left join {{ ref('user_data_int') }} u
  on cpwc.co_id::text = u.user_id
order by cpwc.school_name, u.user_display_name
