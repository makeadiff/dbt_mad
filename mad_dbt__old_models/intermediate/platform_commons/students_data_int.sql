{{ config(materialized='table') }}

SELECT
    "CCI" AS cci,
    "Class" AS class,
    "Course" AS course,
    "Gender" AS gender,
    "School" AS school,
    "Location" AS location,
    "SchoolId" AS school_id,
    "StudentID" AS student_id,
    "SchoolGroup" AS school_group,
    "StudentName" AS student_name,
    "ActiveStatus" AS active_status,
    "StudentRoleNo" AS student_role_no,
    "MediumOfInstruction" AS medium_of_instruction,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time

FROM {{ source('source_platform_commons', 'students_data_int') }}
