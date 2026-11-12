{{ config(materialized='table') }}

-- E1 baseline sheet, tab "Baseline Responses" (E1 spec §3.7, §7 B1): one row per baseline form a
-- volunteer submitted for a child. Every raw column lands as text; renamed and cast here.
--
-- Only the latest _airbyte_generation_id is kept: the stream is full refresh / overwrite now, but an
-- earlier append sync left older generations, so this is a safety net against double counting.
--
-- Personal data: student, volunteer and CHO/CO names and the email address stop here. Nothing
-- downstream of this model may select them (only counts leave the raw layer, B1).
-- Child_ID is free text (no numeric values on 12 Nov); Student_ID is mostly numeric, cast safely.
-- Timestamp is M/D/YYYY H:MM:SS.

with raw as (
    select * from {{ source('google_sheets', 'Baseline_Responses') }}
),

latest as (
    select max("_airbyte_generation_id") as generation_id from raw
)

select
    nullif(trim("Category"), '')                                            as category,
    case when trim("Timestamp") ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$'
         then to_timestamp(trim("Timestamp"), 'FMMM/FMDD/YYYY FMHH24:MI:SS')::timestamp
    end                                                                     as submitted_at,
    nullif(trim("Child_ID"), '')                                            as child_id_text,
    case when trim("Student_ID") ~ '^[0-9]+$' then trim("Student_ID")::bigint end as student_id,
    nullif(trim("Student_ID"), '')                                          as student_id_text,
    nullif(trim("Email_Address"), '')                                       as email_address,
    nullif(trim("Volunteer_Name"), '')                                      as volunteer_name,
    nullif(trim("CHO_CO_Name"), '')                                         as cho_co_name,
    "Chapter_Name"                                                          as chapter_name_raw,
    nullif(trim("Kindly_enter_the_Chapter_name__if_it_s_not_listed_in_t_20971182"), '')
                                                                            as chapter_name_other,
    nullif(trim("Student_Name"), '')                                        as student_name,
    nullif(trim("Student_Grade"), '')                                       as student_grade,
    nullif(trim("Did_the_child_clear_all_the_levels_of_the_ASER_Baseline__"), '')
                                                                            as aser_cleared_answer,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp                                      as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw
where "_airbyte_generation_id" = (select generation_id from latest)
