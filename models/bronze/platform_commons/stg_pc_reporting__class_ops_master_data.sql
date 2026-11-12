{{ config(materialized='table') }}

-- Platform Commons "Class ops master data" (pc_reporting), 1:1 mirror: rename and cast only.
-- One row per slot shift (class = one section x one subject) per academic year, with its weekly
-- slot day. worknode_slot_shift_id (raw "SlotShifId_Class_Id_") joins to
-- stg_pc_worknode_slot_shift; raw "WorknodeSlotId" carries the same value, so it is not a slot id.
-- Academic year comes as 'ACADEMIC_SESSION.2026-2027'. Dates stay text: slot_date is the run date
-- of the export, not a session date.

select
    "SlotShifId_Class_Id_"::bigint  as worknode_slot_shift_id,
    "WorknodeSlotId"::bigint        as worknode_slot_id_raw,
    "AcademicYear"::text            as academic_year,
    "SlotDay"::text                 as slot_day,
    "SlotName"::text                as slot_name,
    "SlotDate"::text                as slot_date,
    "City"::text                    as city,
    "Center"::text                  as center,
    "University"::text              as university,
    "Course"::text                  as course,
    "Stream"::text                  as stream,
    "ClassId"::text                 as class_id,
    "ClassName"::text               as class_name,
    "SectionId"::text               as section_id,
    "SectionName"::text             as section_name,
    "BatchStatus"::text             as batch_status,
    "BatchInactiveReason"::text     as batch_inactive_reason,
    "BatchInactiveStartDate"::text  as batch_inactive_start_date,
    "BatchInactiveEndDate"::text    as batch_inactive_end_date,
    "Mentor"::text                  as mentor,
    "VolunteerId"::text             as volunteer_id,
    "VolunteerAssigned"::text       as volunteer_assigned,
    "UserUpdatedDateTime"::text     as user_updated_date_time,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id
from {{ source('pc_reporting', 'Class_ops_master_data') }}
