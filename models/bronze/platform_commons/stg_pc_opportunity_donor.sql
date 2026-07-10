{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityDonor') }}
)

select
    id::bigint as opportunity_donor_id,
    "opportunityId"::bigint as opportunity_id,
    "name"::text as donor_name,
    "email"::text as donor_email,
    "mobile"::text as donor_mobile,
    "campaign"::text as campaign,
    "medium"::text as medium,
    "isAnonymous"::boolean as is_anonymous,
    "country"::text as country,
    "city"::text as city
from raw
