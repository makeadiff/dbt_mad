{{ config(materialized='table') }}

SELECT
    "EventType" AS event_type,
    "VolunteerId" AS volunteer_id,
    "OpportunityId" AS opportunity_id,
    "VolunteerName" AS volunteer_name,

    CASE
        WHEN "UserSignUpDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserSignUpDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_sign_up_date,

    "VolunteerEmail" AS volunteer_email,
    "OpportunityName" AS opportunity_name,
    "AttendanceStatus" AS attendance_status,

    CASE
        WHEN "InvitationDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("InvitationDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS invitation_date_time,

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

    CASE
        WHEN "AttendanceMarkedOnDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("AttendanceMarkedOnDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS attendance_marked_on_date,

    "OpportunityApplicantId" AS opportunity_applicant_id

FROM {{ source('source_platform_commons', 'events_data_int') }}
