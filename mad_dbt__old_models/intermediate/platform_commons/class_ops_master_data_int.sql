{{ config(materialized='table') }}

SELECT
    "City" AS city,
    "Center" AS center,
    "Course" AS course,
    "Mentor" AS mentor,
    "Stream" AS stream,
    "ClassId" AS class_id,
    "SlotDay" AS slot_day,

    CASE
        WHEN "SlotDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("SlotDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS slot_date,

    "SlotName" AS slot_name,
    "ClassName" AS class_name,
    "SectionId" AS section_id,
    "University" AS university,
    "BatchStatus" AS batch_status,
    "SectionName" AS section_name,
    "VolunteerId" AS volunteer_id,
    "AcademicYear" AS academic_year,
    "WorknodeSlotId" AS worknode_slot_id,
    "VolunteerAssigned" AS volunteer_assigned,
    "BatchInactiveReason" AS batch_inactive_reason,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    CASE
        WHEN "BatchInactiveEndDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("BatchInactiveEndDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS batch_inactive_end_date,

    "SlotShifId_Class_Id_" AS slot_shift_class_id,

    CASE
        WHEN "BatchInactiveStartDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("BatchInactiveStartDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS batch_inactive_start_date

FROM {{ source('source_platform_commons', 'class_ops_master_data_int') }}
