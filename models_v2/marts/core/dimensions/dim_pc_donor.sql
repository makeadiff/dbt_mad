{{ config(materialized='table') }}

with donors as (
    select distinct
        donor_key,
        donor_name,
        donor_email,
        donor_mobile
    from {{ ref('int_pc_fundraising_donations') }}
)

select * from donors
