{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityApplicant') }}
)

select
    id as opportunity_applicant_id,
    "opportunityId" as opportunity_id,
    "linkedUserId" as user_id,
    
    "applicationStatus" as application_status,
    "currentStepCode" as current_step_code,
    "currentStepStatus" as current_step_status,
    
    "applicantCampaign" as applicant_campaign,
    "applicantMedium" as applicant_medium,
    "applicantSource" as applicant_source,
    "applicantReferrer" as applicant_referrer,
    "sourcedByUserId" as sourced_by_user_id,
    
    "appliedToEntityId" as applied_to_entity_id,
    "appliedToEntityType" as applied_to_entity_type,
    
    "applicationDateTime" as application_datetime,
    "applicationSubmitDateTime" as application_submit_datetime,
    "currentStepDateTime" as current_step_datetime,
    "applicationCompleteDateTime" as application_complete_datetime,
    
    "createdAt" as created_at,
    
    "xIsDeleted" as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
