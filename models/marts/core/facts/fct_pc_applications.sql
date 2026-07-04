{{ config(materialized='table') }}

select
    applicant_key,
    user_key,
    opportunity_key,
    "ApplicationID" as application_id,
    "OpportunityId" as opportunity_id,
    "ApplicationStatus" as application_status,
    "CurrentStep" as current_step,
    "CurrentStepStatus" as current_step_status,
    "ApplicationDateTime" as application_datetime,
    "ApplicationSubmitDateTime" as application_submit_datetime,
    "Referrer" as referrer,
    "ReferrerSource" as referrer_source,
    "ReferrerMedium" as referrer_medium,
    "ReferrerCampaign" as referrer_campaign,
    "CodeOfConductPolicyAccepted" as code_of_conduct_policy_accepted,
    "ChildProtectionPolicyAccepted" as child_protection_policy_accepted
from {{ ref('int_pc_applicant_data') }}
