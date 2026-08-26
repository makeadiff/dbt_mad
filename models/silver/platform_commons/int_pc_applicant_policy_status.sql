{{ config(materialized='table') }}

-- Resolves each Platform Commons user's current CPP/COC signing status from their latest application
-- Flow: int_pc_applicant_data -> int_pc_applicant_policy_status
-- int_pc_applicant_data carries CodeOfConductPolicyAccepted/ChildProtectionPolicyAccepted per
-- application (opportunity_applicant_id), so a user with multiple applications has multiple,
-- potentially conflicting values. This dedupes to the latest application per user_id so
-- downstream consumers get one CPP/COC status per user instead of one per application.
--
-- VERIFIED 2026-08-26 against int_pc_applicant_data: CodeOfConductPolicyAccepted and
-- ChildProtectionPolicyAccepted only ever appear as 'true' or blank/null in raw data -- no
-- 'yes'/'1'/'t' variants exist today. The broader truthy-value list is kept as a defensive
-- superset, not because those other values have been observed.

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
