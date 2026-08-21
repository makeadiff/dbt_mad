{{ config(materialized='table') }}

-- Resolves each Platform Commons user's current CPP/COC signing status from their latest application
-- Flow: int_pc_applicant_data -> int_pc_applicant_policy_status
-- int_pc_applicant_data carries CodeOfConductPolicyAccepted/ChildProtectionPolicyAccepted per
-- application (opportunity_applicant_id), so a user with multiple applications has multiple,
-- potentially conflicting values. This dedupes to the latest application per user_id so
-- downstream consumers get one CPP/COC status per user instead of one per application.
--
-- ASSUMPTION (unverified against raw data -- no DB access at time of writing): the raw
-- cocAccepted/policyAccepted columns are treated as accepted when their text value is one of
-- 'true'/'yes'/'1'/'t' (case-insensitive, trimmed). Confirm actual raw values in
-- pc_raw."opportunityApplicantMeta" before relying on this in a dashboard.

with applications as (
    select
        "UserId" as user_id,
        "ApplicationDateTime" as application_datetime,
        "CodeOfConductPolicyAccepted" as code_of_conduct_policy_accepted_raw,
        "ChildProtectionPolicyAccepted" as child_protection_policy_accepted_raw,
        row_number() over (
            partition by "UserId"
            order by "ApplicationDateTime" desc nulls last
        ) as rn
    from {{ ref('int_pc_applicant_data') }}
    where "UserId" is not null
)

select
    user_id,
    lower(trim(code_of_conduct_policy_accepted_raw)) in ('true', 'yes', '1', 't') as coc_accepted,
    lower(trim(child_protection_policy_accepted_raw)) in ('true', 'yes', '1', 't') as cpp_accepted
from applications
where rn = 1
