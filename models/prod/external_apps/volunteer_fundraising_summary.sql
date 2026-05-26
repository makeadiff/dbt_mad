{{ config(materialized='table') }}

with school_volunteer as (
    select * from {{ ref('school_volunteer_int') }}
),
user_data as (
    select * from {{ ref('user_data') }}
),
partner_data as (
    select * from {{ ref('partner_int') }} where removed = false
),
slot_class_section_volunteer as (
    select * from {{ ref('slot_class_section_volunteer_int') }}
),
slot_class_section as (
    select * from {{ ref('slot_class_section_int') }}
),
slot_data as (
    select * from {{ ref('slot_int') }}
),
fundraising_summary as (
    select
        fundraiser_id::numeric as volunteer_id,
        min(payment_date) as first_donation_date,
        sum(total_amount_paid) as total_amount_raised,
        bool_or(donation_type = 'recurring') as raised_recurring_donation
    from {{ ref('fundraising_donations') }}
    group by fundraiser_id::numeric
)

select
    -- volunteer id from school_volunteer_int
    svi.volunteer_id,

    -- volunteer details
    ud.user_display_name as volunteer_name,
    ud.email as volunteer_email,
    ud.user_login as volunteer_user_login,
    ud.contact as volunteer_contact,

    -- school / partner details
    pi.partner_name,
    pi.co_name,
    pi.city as partner_city,
    pi.partner_id1 as partner_id,

    -- slot info (nullable)
    s.slot_name,

    -- fundraising summary
    fd.first_donation_date,
    coalesce(fd.total_amount_raised, 0) as total_amount_raised,
    (coalesce(fd.total_amount_raised, 0) >= 500) as raised_first_500,
    coalesce(fd.raised_recurring_donation, false) as raised_recurring_donation

from school_volunteer svi

-- volunteer master data
inner join user_data ud
    on svi.volunteer_id::text = ud.user_id::text

-- school / partner details
inner join partner_data pi
    on svi.school_id = pi.partner_id1

-- slot assignments (nullable)
left join slot_class_section_volunteer scsv
    on scsv.volunteer_id = svi.volunteer_id
   and scsv.removed = false

left join slot_class_section scs
    on scs.slot_class_section_id = scsv.slot_class_section_id

left join slot_data s
    on s.slot_id = scs.slot_id

-- fundraising summary
left join fundraising_summary fd
    on fd.volunteer_id = svi.volunteer_id

where svi.removed = false

order by pi.co_name asc
