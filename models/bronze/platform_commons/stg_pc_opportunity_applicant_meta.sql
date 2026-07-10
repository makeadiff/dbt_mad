{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityApplicantMeta') }}
)

select
    id::bigint as meta_id,
    "opportunityApplicantId"::bigint as opportunity_applicant_id,
    "areaOfResidence"::text as area_of_residence,
    "cocAccepted"::text as code_of_conduct_policy_accepted,
    "policyAccepted"::text as child_protection_policy_accepted,
    "policyAccepted"::text as policy_accepted,
    nullif("currentUserType", '')::json->>'name' as current_user_type
from raw
