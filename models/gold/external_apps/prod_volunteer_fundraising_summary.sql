{{ config(materialized='table') }}

with school_volunteer as (
    select * from {{ ref('prod_school_volunteer') }}
),
slot_class_section_volunteer as (
    select * from {{ ref('int_bubble__slot_class_section_volunteer') }}
),
slot_class_section as (
    select * from {{ ref('int_bubble__slot_class_section') }}
),
slot_data as (
    select * from {{ ref('dim_slot') }}
),
fundraising_summary as (
    select
        fundraiser_id::numeric as volunteer_id,
        min(payment_date) as first_donation_date,
        sum(total_amount_paid) as total_amount_raised,
        bool_or(donation_type = 'recurring') as raised_recurring_donation
    from {{ ref('prod_fundraising_donations') }}
    group by fundraiser_id::numeric
),
active_volunteers as (
    select distinct volunteer_id::numeric as volunteer_id from school_volunteer where removed = false
    union
    select distinct volunteer_id::numeric as volunteer_id from fundraising_summary
)

select
    av.volunteer_id,
    coalesce(sv.user_display_name, ud.user_display_name) as volunteer_name,
    ud.email as volunteer_email,
    coalesce(sv.user_login, ud.user_login) as volunteer_user_login,
    coalesce(sv.contact_number, ud.contact) as volunteer_contact,
    sv.partner_name,
    sv.partner_co_name as co_name,
    sv.partner_city,
    sv.partner_id,
    s.slot_name,
    fd.first_donation_date,
    coalesce(fd.total_amount_raised, 0) as total_amount_raised,
    (coalesce(fd.total_amount_raised, 0) >= 500) as raised_first_500,
    coalesce(fd.raised_recurring_donation, false) as raised_recurring_donation

from active_volunteers av
left join school_volunteer sv
    on av.volunteer_id = sv.volunteer_id::numeric
   and sv.removed = false
left join {{ ref('prod_user_data') }} ud
    on av.volunteer_id = ud.user_id::numeric
left join slot_class_section_volunteer scsv
    on scsv.volunteer_id = av.volunteer_id
   and scsv.is_removed = false
left join slot_class_section scs
    on scs.slot_class_section_id = scsv.slot_class_section_id
left join slot_data s
    on s.slot_id = scs.slot_id
left join fundraising_summary fd
    on fd.volunteer_id = av.volunteer_id
order by sv.partner_co_name asc nulls last
