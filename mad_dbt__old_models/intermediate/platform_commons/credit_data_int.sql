{{ config(materialized='table') }}

SELECT
    "UserId" AS user_id,
    "UserRoles" AS user_roles,
    "CreditPoint" AS credit_point,
    "TaggedAtName" AS tagged_at_name,
    "TaggedAtType" AS tagged_at_type,

    CASE
        WHEN "OnBoardedDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("OnBoardedDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS onboarded_date,

    "VolunteerName" AS volunteer_name,
    "CreditPointHistory" AS credit_point_history,
    "ParentTaggedAtName" AS parent_tagged_at_name,
    "ParentTaggedAtType" AS parent_tagged_at_type,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    "CreditPointHistoryReason" AS credit_point_history_reason

FROM {{ source('source_platform_commons', 'credit_data_int') }}
