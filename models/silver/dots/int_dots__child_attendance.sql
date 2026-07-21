{{ config(materialized='table') }}

-- Extracts child attendance fields out of DOTS' nested JSON (main/tags/meta) payload
-- Flow: stg_dots__child_attendance → int_dots__child_attendance
-- Replaces the legacy dots_child_attendance_int (flat google_sheet export) now that DOTS
-- lands as Bubble/Knack-style jsonb blobs; column shape is kept the same for downstream compatibility.
-- childAttendance carries no survey questions at all (q1-q4 were already nulled out in the legacy
-- model for the same reason), and end_date, substitute volunteer name/MAD ID, the substituted-by
-- flag, main_subtitle and main_cover_image have no equivalent anywhere in the DOTS raw feed
-- (checked: empty/absent on every row) — nulled out below rather than dropped, so downstream
-- consumers keep the column.
-- No business column (or combination) is unique at this grain — e.g. the same child/volunteer/slot
-- can have multiple submissions for the same current_week/survey_cycle (corrections/resubmissions) —
-- so record_id (the raw document _id) is carried through as the actual row-level primary key.

with source_data as (
    select * from {{ ref('stg_dots__child_attendance') }}
)

select
    -- Identifiers
    "_id"                                                                    as record_id,
    nullif(trim(tags #>> '{childId,data,0,tagId}'), '')::integer             as child_id,
    nullif(trim(tags #>> '{partnerId,data,0,tagId}'), '')::integer           as partner_id,
    nullif(trim(tags #>> '{volunteerId,data,0,tagId}'), '')::integer         as volunteer_id,
    nullif(trim(tags #>> '{slotClassSectionId,data,0,tagId}'), '')::integer  as slot_class_section_id,

    -- Names
    nullif(trim(tags #>> '{childName,data,0,display}'), '')     as child_name,
    nullif(trim(tags #>> '{partnerName,data,0,display}'), '')   as partner_name,
    nullif(trim(tags #>> '{volunteers,data,0,display}'), '')    as volunteers,

    -- Attendance info
    nullif(trim(main #>> '{childAttendance_attendance,value}'), '')      as attendance,
    nullif(trim(main #>> '{childAttendance_day_of_week,value}'), '')     as day_of_week,
    nullif(trim(main #>> '{childAttendance_currentweek,value}'), '')::integer  as current_week,
    nullif(trim(main #>> '{childAttendance_surveyCycle,value}'), '')::integer  as survey_cycle,

    -- Dates (DOTS always emits ISO-8601, unlike the old google sheet export)
    nullif(trim(main ->> 'childAttendance_start_date'), '')::timestamp             as start_date,
    null::timestamp                                                                 as end_date,
    nullif(trim(main ->> 'childAttendance_entryStartedDate'), '')::timestamp       as entry_started_date,
    nullif(trim(main ->> 'childAttendance_entryLastUpdatedDate'), '')::timestamp   as entry_last_updated_date,

    -- Substitute info (not populated anywhere in the DOTS raw feed)
    null::text  as substitute_volunteer_name,
    null::text  as substitute_volunteer_mad_id,
    'N/A'::text as volunteer_was_substituted,

    -- Survey answers (not present in the childAttendance raw feed)
    null::text as q1,
    null::text as q2,
    null::text as q3,
    null::text as q4,

    -- Form metadata
    nullif(trim(main ->> 'title'), '')              as title,
    nullif(trim(tag_id), '')                        as tag_id,
    nullif(trim(meta ->> 'kp_contributed_by'), '')  as author,
    null::text                                      as main_subtitle,
    null::text                                      as main_cover_image,
    nullif(trim(kp_date_published), '')::date           as published_date,
    nullif(trim(last_activity ->> 'date'), '')::date    as last_activity_date

from source_data
where nullif(trim(tags #>> '{childId,data,0,tagId}'), '') is not null
  and nullif(trim(tags #>> '{partnerId,data,0,tagId}'), '') is not null
  and nullif(trim(tags #>> '{volunteerId,data,0,tagId}'), '') is not null
  and nullif(trim(main #>> '{childAttendance_attendance,value}'), '') is not null
  and nullif(trim(main ->> 'childAttendance_start_date'), '') is not null
