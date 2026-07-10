{{ config(materialized='table') }}

SELECT
    "schoolId" AS school_id,
    "IsActive" AS is_active,
    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time

FROM {{ source('source_platform_commons', 'school_id_int') }}
