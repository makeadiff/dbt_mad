{{ config(materialized='table') }}

SELECT
    "User_Id" AS user_id,
    "Full_Name" AS full_name,
    "RetentionId" AS retention_id,
    "Current_Roles" AS current_roles,
    "Email_Address" AS email_address,
    "Mobile_Number" AS mobile_number,
    "Preferred_Role" AS preferred_role,
    "Retention_Year" AS retention_year,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    "Willing_To_Continue" AS willing_to_continue,
    "Current_WorkNode_Name" AS current_work_node_name,
    "Current_WorkNode_Type" AS current_work_node_type,
    "Preferred_WorkNode_Name" AS preferred_work_node_name,
    "Preferred_WorkNode_Type" AS preferred_work_node_type,

    CASE
        WHEN "Retention_Sent_Date_Time" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("Retention_Sent_Date_Time", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS retention_sent_date_time,

    "Reasons_For_Not_Continuing" AS reasons_for_not_continuing

FROM {{ source('source_platform_commons', 'volunteer_retention_d_int') }}
