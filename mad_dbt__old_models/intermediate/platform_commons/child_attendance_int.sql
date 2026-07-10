{{ config(materialized='table') }}

SELECT
    "Class" AS class,
    "Course" AS course,
    "Gender" AS gender,
    "Reason" AS reason,
    "Stream" AS stream,
    "ChildId" AS child_id,
    "Section" AS section,
    "CityName" AS city_name,
    "SchoolId" AS school_id,
    "ChildName" AS child_name,
    "SectionId" AS section_id,
    "CenterName" AS center_name,
    "RequestType" AS request_type,
    "SubjectCode" AS subject_code,
    "batchStatus" AS batch_status,
    "CenterSlotId" AS center_slot_id,
    "SlotMentorId" AS slot_mentor_id,
    "SubstituteId" AS substitute_id,
    "CenterSlotName" AS center_slot_name,
    "CenterSlotEndTime" AS center_slot_end_time,
    "SubstitutedUserId" AS substituted_user_id,
    "SectionSlotShiftId" AS section_slot_shift_id,
    "TaggedVolunteerIds" AS tagged_volunteer_ids,
    "CenterSlotDayOfWeek" AS center_slot_day_of_week,
    "CenterSlotStartTime" AS center_slot_start_time,

    CASE
        WHEN "SlotCreatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("SlotCreatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS slot_created_date_time,

    CASE
        WHEN "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("UserUpdatedDateTime", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS user_updated_date_time,

    CASE
        WHEN "ScheduledSessionDate" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP("ScheduledSessionDate", 'YYYY-MM-DD"T"HH24:MI:SS')
        ELSE NULL
    END AS scheduled_session_date,

    "ChildAttendanceStatus" AS child_attendance_status,
    "AttendanceTakenByUserId" AS attendance_taken_by_user_id,
    "Reason_for_the_child_s_absence" AS reason_for_the_childs_absence,
    "Did_the_student_complete_the_assigned_work_or_task_s__" AS did_complete_assigned_task,
    "Did_the_student_actively_participate_in_the_class_today_" AS did_actively_participate,
    "Did_the_student_understand_the_concept_s__taught_in_class_today" AS did_understand_concepts,
    "Is_there_anything_else_to_note_about_the_child_s_performance_or" AS additional_notes

FROM {{ source('source_platform_commons', 'child_attendance_int') }}
