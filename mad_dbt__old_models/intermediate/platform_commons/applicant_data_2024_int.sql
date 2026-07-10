{{ config(materialized='table') }}

SELECT
    "City" AS city,
    "State" AS state,
    "Gender" AS gender,
    "UserId" AS user_id,
    "Country" AS country,
    "Pincode" AS pincode,
    "AgeGroup" AS age_group,
    "LastName" AS last_name,
    "Referrer" AS referrer,
    "FirstName" AS first_name,
    "CurrentStep" AS current_step,

    CASE
        WHEN "DateOfBirth" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("DateOfBirth", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS date_of_birth,

    "DisplayName" AS display_name,
    "MobileNumber" AS mobile_number,
    "RoleAssigned" AS role_assigned,
    "AboutYourself" AS about_yourself,
    "ApplicationID" AS application_id,

    CASE
        WHEN "DateOfJoining" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("DateOfJoining", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS date_of_joining,

    "HowDidYouHear" AS how_did_you_hear,
    "OpportunityId" AS opportunity_id,
    "ReferrerLogin" AS referrer_login,
    "CurrentlyDoing" AS currently_doing,
    "ReferrerMedium" AS referrer_medium,
    "ReferrerSource" AS referrer_source,
    "WorknodeStatus" AS worknode_status,
    "SourcedByUserId" AS sourced_by_user_id,
    "ReferrerCampaign" AS referrer_campaign,
    "ApplicationStatus" AS application_status,
    "CurrentStepStatus" AS current_step_status,

    CASE
        WHEN "ApplicationDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ApplicationDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS application_date_time,

    "PrimaryEmailAddress" AS primary_email_address,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    "AppliedToWorkNodeName" AS applied_to_work_node_name,
    "AppliedToWorkNodeType" AS applied_to_work_node_type,
    "SelectedForWorkNodeName" AS selected_for_work_node_name,
    "SelectedForWorkNodeType" AS selected_for_work_node_type,

    CASE
        WHEN "ApplicationSubmitDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ApplicationSubmitDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS application_submit_date_time,

    "SelectedForParentWorkNode" AS selected_for_parent_work_node,
    "VolunteeringPreferredDays" AS volunteering_preferred_days,
    "AppliedToParentWorkNodeName" AS applied_to_parent_work_node_name,
    "AppliedToParentWorkNodeType" AS applied_to_parent_work_node_type,
    "CodeOfConductPolicyAccepted" AS code_of_conduct_policy_accepted,
    "DistanceFromCollegeToCCI_Km_" AS distance_from_college_to_cci_km,
    "ChildProtectionPolicyAccepted" AS child_protection_policy_accepted,
    "SelectedForParentWorkNodeType" AS selected_for_parent_work_node_type

FROM {{ source('source_platform_commons', 'applicant_data_2024_int') }}
