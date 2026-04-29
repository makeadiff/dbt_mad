{{ config(
    materialized='table'
) }}

with applicant as (
    select * from {{ ref('stg_pc_opportunity_applicant') }}
),

users as (
    select * from {{ ref('stg_pc_user') }}
),

profile as (
    select * from {{ ref('stg_pc_person_profile') }}
)

select
    -- Identifier mapping to replicate Applicant_Data_2025
    a.opportunity_applicant_id as "ApplicationID",
    a.opportunity_id as "OpportunityId",
    a.user_id as "UserId",
    
    -- Status and Dates
    a.application_status as "ApplicationStatus",
    a.current_step_code as "CurrentStep",
    a.current_step_status as "CurrentStepStatus",
    a.application_datetime as "ApplicationDateTime",
    a.application_submit_datetime as "ApplicationSubmitDateTime",
    
    -- User Info
    u.login as "PrimaryEmailAddress",
    u.first_name as "FirstName",
    u.last_name as "LastName",
    u.first_name || ' ' || coalesce(u.last_name, '') as "DisplayName",
    
    -- Profile Demographic
    p.date_of_birth as "DateOfBirth",
    p.gender_identifier as "Gender",
    p.current_professional_status as "CurrentlyDoing",
    
    -- Application Source
    a.applicant_referrer as "Referrer",
    a.applicant_source as "ReferrerSource",
    a.applicant_medium as "ReferrerMedium",
    a.applicant_campaign as "ReferrerCampaign",
    a.sourced_by_user_id as "SourcedByUserId",
    
    -- Worknode Data Mapped
    a.applied_to_entity_id as "AppliedToWorknodeId",
    a.applied_to_entity_type as "AppliedToWorknodeType",
    
    current_timestamp as "_airbyte_extracted_at"

from applicant a
left join users u on a.user_id = u.user_id
left join profile p on u.person_id = p.person_profile_id
where a.is_deleted = false
