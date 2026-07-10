{{ config(materialized='table') }}

SELECT
    "Gender" AS gender,
    "UserId" AS user_id,
    "ApplicantId" AS applicant_id,
    "CurrentStep" AS current_step,

    CASE
        WHEN "JoiningDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("JoiningDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS joining_date,

    "ApplicantName" AS applicant_name,
    "SourcedMedium" AS sourced_medium,
    "SourcedSource" AS sourced_source,
    "ApplicantEmail" AS applicant_email,

    CASE
        WHEN "UserSignUpDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserSignUpDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_sign_up_date,

    "CurrentUserType" AS current_user_type,
    "SourcedCampaign" AS sourced_campaign,
    "RolesPlayedInMAD" AS roles_played_in_mad,
    "ApplicationStatus" AS application_status,
    "CurrentStepStatus" AS current_step_status,

    CASE
        WHEN "ApplicationDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ApplicationDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS application_date_time,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    "CurrentRolesAssigned" AS current_roles_assigned,
    "PrimaryContactNumber" AS primary_contact_number,
    "AppliedToWorkNodeName" AS applied_to_work_node_name,
    "AppliedToWorkNodeType" AS applied_to_work_node_type,
    "WhatsappContactNumber" AS whatsapp_contact_number,
    "TotalYearsOfExperience" AS total_years_of_experience,
    "NumbersOfFeedbacksGiven" AS numbers_of_feedbacks_given,
    "SelectedForWorkNodeName" AS selected_for_work_node_name,
    "SelectedForWorkNodeType" AS selected_for_work_node_type,
    "CurrentWorkNodesAssigned" AS current_work_nodes_assigned,
    "SelectedForParentWorkNode" AS selected_for_parent_work_node,
    "SelectedForParentWorkNodeType" AS selected_for_parent_work_node_type

FROM {{ source('source_platform_commons', 'fellow_applicant_data_int') }}
