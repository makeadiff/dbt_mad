{{ config(materialized='table', tags=["user_data"]) }}

SELECT
    "City" AS city,
    "Email" AS email,
    "State" AS state,
    "Center" AS center,
    "UserId"::text AS user_id,
    "AddedBy" AS added_by,
    "Contact" AS contact,
    "UserRole" AS user_role,
    "UserLogin" AS user_login,
    "UserDisplayName" AS user_display_name,
    "ReportingManagerUserId"::int AS reporting_manager_user_id,
    "ReportingManagerRoleCode" AS reporting_manager_role_code,
    "ReportingManagerUserLogin" AS reporting_manager_user_login,
    "UserCreatedDateTime" AS user_created_datetime,
    "UserUpdatedDateTime" AS user_updated_datetime
FROM {{ ref('int_pc_user_data') }}
WHERE "UserRole" IN (
    'CO Part Time',
    'Wingman',
    'Academic Support',
    'Function Lead',
    'CO Full Time',
    'CHO',
    'Project Associate',
    'Fellow',
    'Project Lead',
    'Youth'
)
AND "IsActive" = true
