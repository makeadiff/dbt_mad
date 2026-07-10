{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityApplicant') }}
)

select
    id::bigint as opportunity_applicant_id,
    "opportunityId"::bigint as opportunity_id,
    "linkedUserId"::bigint as user_id,
    "marketUserId"::bigint as market_user_id,
    
    "applicationStatus"::text as application_status,
    "currentStepCode"::text as current_step_code,
    "currentStepStatus"::text as current_step_status,
    
    "applicantCampaign"::text as applicant_campaign,
    "applicantMedium"::text as applicant_medium,
    "applicantSource"::text as applicant_source,
    "applicantReferrer"::text as applicant_referrer,
    
    "appliedToEntityId"::bigint as applied_to_entity_id,
    "appliedToEntityType"::text as applied_to_entity_type,
    "secondaryAppliedToWorknodeId"::bigint as secondary_applied_to_worknode_id,
    "forRole"::text as for_role,
    
    "applicationDateTime"::timestamp as application_datetime,
    "applicationSubmitDateTime"::timestamp as application_submit_datetime,
    "currentStepDateTime"::timestamp as current_step_datetime,
    case
        when "applicationCompleteDateTime" ~ '^[0-9]{10,}$'
        then (to_timestamp("applicationCompleteDateTime"::bigint / 1000.0) at time zone 'Asia/Kolkata')
        else "applicationCompleteDateTime"::timestamp
    end as application_complete_datetime,
    case
        when "attendanceMarkedOn" ~ '^[0-9]{10,}$'
        then (to_timestamp("attendanceMarkedOn"::bigint / 1000.0) at time zone 'Asia/Kolkata')
        else "attendanceMarkedOn"::timestamp
    end as attendance_marked_on,
    "contactNumber"::text as contact_number,
    "dateOfJoining"::text as date_of_joining,
    
    "createdAt"::text as created_at,
    
    "xIsDeleted"::boolean as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
