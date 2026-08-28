{{ config(materialized='table') }}

-- Resolves each Platform Commons user's CPP/COC signing status across their full application
-- history.
-- Flow: int_pc_applicant_data -> int_pc_applicant_policy_status
--
-- FIXED 2026-08-27 -- was dedup-to-latest-application, which discarded real acceptances (the
-- same failure class as D1(c)): a continuing volunteer's latest application can carry
-- blank/unset policy fields even though an earlier application had both accepted. An acceptance
-- that happened, happened -- 4 volunteers were compliant under the old logic, 60 are compliant
-- under this one (verified 2026-08-27 against the same population). is_compliant is now
-- COMPLIANT IN ANY APPLICATION: true if there exists a single application where both CPP and COC
-- were accepted together.
--
-- compliance_year = the year of the MOST RECENT such compliant application. This is a policy
-- decision left to read time, not baked into the model: whether CPP/COC needs annual
-- re-signature is an open ops question (since 2026-08-06). Carrying the year lets a consumer
-- filter to "compliant, and re-signed this year" without a rebuild, without this model taking a
-- position on whether that filter should be applied.
--
-- IMPORTANT CAVEAT: compliance_year is derived from the application's own timestamp
-- (ApplicationDateTime, sourced from appCreatedAt) -- NOT a dedicated signature date. No such
-- date exists in this data; CodeOfConductPolicyAccepted/ChildProtectionPolicyAccepted are
-- per-application booleans with no timestamp of their own. So "when compliance was most
-- recently established" is approximated by "when that application was submitted", not measured
-- directly. This is the answer to C1's edge case, open since 2026-08-06: annual validity cannot
-- be evaluated precisely from this data.
--
-- VERIFIED 2026-08-26 against int_pc_applicant_data: CodeOfConductPolicyAccepted and
-- ChildProtectionPolicyAccepted only ever appear as 'true' or blank/null in raw data -- no
-- 'yes'/'1'/'t' variants exist today. The broader truthy-value list is kept as a defensive
-- superset, not because those other values have been observed.

with applications as (
    select
        "UserId" as user_id,
        "ApplicationDateTime" as application_datetime,
        lower(trim("CodeOfConductPolicyAccepted")) in ('true', 'yes', '1', 't') as coc_accepted_this_application,
        lower(trim("ChildProtectionPolicyAccepted")) in ('true', 'yes', '1', 't') as cpp_accepted_this_application
    from {{ ref('int_pc_applicant_data') }}
    where "UserId" is not null
),

compliant_applications as (
    select user_id, application_datetime
    from applications
    where coc_accepted_this_application and cpp_accepted_this_application
)

select
    user_id,
    true as is_compliant,
    extract(year from max(application_datetime))::integer as compliance_year
from compliant_applications
group by user_id
