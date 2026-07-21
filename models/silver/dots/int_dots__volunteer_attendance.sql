{{ config(materialized='table') }}

-- Extracts volunteer attendance + survey fields out of DOTS' nested JSON (main/tags/meta) payload
-- Flow: stg_dots__volunteer_attendance → int_dots__volunteer_attendance
-- Replaces the legacy dots_volunteer_attendance_int (flat google_sheet export) now that DOTS
-- lands as Bubble/Knack-style jsonb blobs; column shape is kept the same for downstream compatibility.
-- end_date, substitute volunteer name/MAD ID, the substituted-by flag, main_subtitle and
-- main_cover_image have no equivalent anywhere in the DOTS raw feed (checked: empty/absent on
-- every row) and are nulled out below rather than dropped, so downstream consumers keep the column.
-- No business column (or combination) is unique at this grain — e.g. the same volunteer/partner/slot
-- can have multiple submissions for the same current_week/survey_cycle (corrections/resubmissions) —
-- so record_id (the raw document _id) is carried through as the actual row-level primary key.

with source_data as (
    select * from {{ ref('stg_dots__volunteer_attendance') }}
)

select
    -- Identifiers
    "_id"                                                                    as record_id,
    nullif(trim(tags #>> '{partnerId,data,0,tagId}'), '')::integer            as partner_id,
    nullif(trim(tags #>> '{volunteerId,data,0,tagId}'), '')::integer         as volunteer_id,
    nullif(trim(tags #>> '{slotClassSectionId,data,0,tagId}'), '')::integer  as slot_class_section_id,

    -- Names
    nullif(trim(tags #>> '{partnerName,data,0,display}'), '')  as partner_name,
    nullif(trim(tags #>> '{volunteers,data,0,display}'), '')   as volunteers,

    -- Attendance info
    nullif(trim(main #>> '{volunteerAttendance_attendance,value}'), '')      as attendance,
    nullif(trim(main #>> '{volunteerAttendance_day_of_week,value}'), '')     as day_of_week,
    nullif(trim(main #>> '{volunteerAttendance_currentweek,value}'), '')::integer   as current_week,
    nullif(trim(main #>> '{volunteerAttendance_surveyCycle,value}'), '')::integer   as survey_cycle,
    nullif(trim(main ->> 'volunteerAttendance_feedback'), '')                as feedback,

    -- Dates (DOTS always emits ISO-8601, unlike the old google sheet export)
    nullif(trim(main ->> 'volunteerAttendance_start_date'), '')::timestamp             as start_date,
    null::timestamp                                                                     as end_date,
    nullif(trim(main ->> 'volunteerAttendance_entryStartedDate'), '')::timestamp       as entry_started_date,
    nullif(trim(main ->> 'volunteerAttendance_entryLastUpdatedDate'), '')::timestamp   as entry_last_updated_date,

    -- Substitute info (not populated anywhere in the DOTS raw feed)
    null::text  as substitute_volunteer_name,
    null::text  as substitute_volunteer_mad_id,
    'N/A'::text as volunteer_was_substituted,

    -- Survey answers
    nullif(trim(main ->> 'volunteerAttendance_q5'), '')      as q5_reflections,

    -- Survey set 1
    nullif(trim(main ->> 'volunteerAttendance_q1_1'), '')    as q1_set_1,
    nullif(trim(main ->> 'volunteerAttendance_q2_1'), '')    as q2_set_1,
    nullif(trim(main ->> 'volunteerAttendance_q3_1'), '')    as q3_set_1,
    nullif(trim(main ->> 'volunteerAttendance_q4_1'), '')    as q4_set_1,

    -- Survey set 2
    nullif(trim(main ->> 'volunteerAttendance_q1_2'), '')    as q1_set_2,
    nullif(trim(main ->> 'volunteerAttendance_q2_2'), '')    as q2_set_2,
    nullif(trim(main ->> 'volunteerAttendance_q3_2'), '')    as q3_set_2,
    nullif(trim(main ->> 'volunteerAttendance_q4_2'), '')    as q4_set_2,

    -- Survey set 3
    nullif(trim(main ->> 'volunteerAttendance_q1_3'), '')    as q1_set_3,
    nullif(trim(main ->> 'volunteerAttendance_q2_3'), '')    as q2_set_3,
    nullif(trim(main ->> 'volunteerAttendance_q3_3'), '')    as q3_set_3,
    nullif(trim(main ->> 'volunteerAttendance_q4_3'), '')    as q4_set_3,

    -- Survey set 4
    nullif(trim(main ->> 'volunteerAttendance_q1_4'), '')    as q1_set_4,
    nullif(trim(main ->> 'volunteerAttendance_q2_4'), '')    as q2_set_4,
    nullif(trim(main ->> 'volunteerAttendance_q3_4'), '')    as q3_set_4,
    nullif(trim(main ->> 'volunteerAttendance_q4_4'), '')    as q4_set_4,

    -- Form metadata
    nullif(trim(main ->> 'title'), '')          as title,
    nullif(trim(tag_id), '')                    as tag_id,
    nullif(trim(meta ->> 'kp_contributed_by'), '')  as author,
    null::text                                  as main_subtitle,
    null::text                                  as main_cover_image,
    nullif(trim(kp_date_published), '')::date       as published_date,
    nullif(trim(last_activity ->> 'date'), '')::date as last_activity_date

from source_data
where nullif(trim(tags #>> '{partnerId,data,0,tagId}'), '') is not null
  and nullif(trim(tags #>> '{volunteerId,data,0,tagId}'), '') is not null
  and nullif(trim(main #>> '{volunteerAttendance_attendance,value}'), '') is not null
  and nullif(trim(main ->> 'volunteerAttendance_start_date'), '') is not null
