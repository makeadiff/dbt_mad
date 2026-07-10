{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('google_sheet_staging', 'child_attendance') }}
)

SELECT
  -- Identifiers
  NULLIF(TRIM("Child_ID"), '')::integer               AS child_id,
  NULLIF(TRIM("Partner_ID"), '')::integer             AS partner_id,
  NULLIF(TRIM("Volunteer_ID"), '')::integer           AS volunteer_id,
  NULLIF(TRIM("Slot_Class_Section_ID"), '')::integer   AS slot_class_section_id,

  -- Names
  NULLIF(TRIM("Child_Name"), '')                      AS child_name,
  NULLIF(TRIM("Partner_Name"), '')                    AS partner_name,
  NULLIF(TRIM("volunteers"), '')                      AS volunteers,

  -- Attendance info
  NULLIF(TRIM("Attendance"), '')                      AS attendance,
  NULLIF(TRIM("Day_Of_Week"), '')                     AS day_of_week,
  NULLIF(TRIM("Current_Week"), '')::integer            AS current_week,
  NULLIF(TRIM("Survey_Cycle"), '')::integer           AS survey_cycle,

  -- Dates
  CASE
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'
    THEN TO_TIMESTAMP(TRIM("Start_Date"), 'DD-MM-YYYY HH24:MI:SS')
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("Start_Date"), 10), 'DD-MM-YYYY')::timestamp
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("Start_Date")::timestamp
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("Start_Date"), 10)::timestamp
    ELSE NULL
  END                                                 AS start_date,
  CASE
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'
    THEN TO_TIMESTAMP(TRIM("End_Date"), 'DD-MM-YYYY HH24:MI:SS')
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("End_Date"), 10), 'DD-MM-YYYY')::timestamp
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("End_Date")::timestamp
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("End_Date"), 10)::timestamp
    ELSE NULL
  END                                                 AS end_date,
  CASE
    WHEN TRIM("Entry_Started_Date") ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'
    THEN TO_TIMESTAMP(TRIM("Entry_Started_Date"), 'DD-MM-YYYY HH24:MI:SS')
    WHEN TRIM("Entry_Started_Date") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Started_Date")::timestamp
    WHEN TRIM("Entry_Started_Date") ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Started_Date")::timestamp
    ELSE NULL
  END                                                 AS entry_started_date,
  CASE
    WHEN TRIM("Entry_Last_Updated_Date") ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'
    THEN TO_TIMESTAMP(TRIM("Entry_Last_Updated_Date"), 'DD-MM-YYYY HH24:MI:SS')
    WHEN TRIM("Entry_Last_Updated_Date") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Last_Updated_Date")::timestamp
    WHEN TRIM("Entry_Last_Updated_Date") ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Last_Updated_Date")::timestamp
    ELSE NULL
  END                                                 AS entry_last_updated_date,

  -- Substitute info
  NULLIF(TRIM("Substitute_Volunteer_Name"), '')       AS substitute_volunteer_name,
  NULLIF(TRIM("Substitute_Volunteer_MAD_ID"), '')     AS substitute_volunteer_mad_id,
  CASE
    WHEN LOWER(TRIM("Volunteer_was_substituted_by_someone_else_")) IN ('yes', 'true', '1') THEN 'Yes'
    WHEN LOWER(TRIM("Volunteer_was_substituted_by_someone_else_")) IN ('no', 'false', '0') THEN 'No'
    ELSE 'N/A'
  END                                                 AS volunteer_was_substituted,

  -- Survey answers (columns not yet present in source sheet)
  NULL::text                                          AS q1,
  NULL::text                                          AS q2,
  NULL::text                                          AS q3,
  NULL::text                                          AS q4,

  -- Form metadata
  NULLIF(TRIM("Title"), '')                           AS title,
  NULLIF(TRIM("tagId"), '')                           AS tag_id,
  NULLIF(TRIM("Author"), '')                          AS author,
  NULLIF(TRIM("main_subtitle"), '')                   AS main_subtitle,
  NULLIF(TRIM("main_coverImage"), '')                 AS main_cover_image,
  CASE
    WHEN NULLIF(TRIM("Published_Date"), '') ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("Published_Date"), 10), 'DD-MM-YYYY')
    WHEN NULLIF(TRIM("Published_Date"), '') ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("Published_Date"), 10)::date
    ELSE NULL
  END                                                 AS published_date,
  CASE
    WHEN TRIM("lastActivity_date") ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("lastActivity_date"), 10), 'DD-MM-YYYY')
    WHEN TRIM("lastActivity_date") ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("lastActivity_date"), 10)::date
    ELSE NULL
  END                                                 AS last_activity_date,

  -- Airbyte metadata
  _airbyte_raw_id,
  _airbyte_extracted_at,
  _airbyte_meta

FROM source_data
WHERE NULLIF(TRIM("Child_ID"), '') IS NOT NULL
  AND NULLIF(TRIM("Partner_ID"), '') IS NOT NULL
  AND NULLIF(TRIM("Volunteer_ID"), '') IS NOT NULL
  AND NULLIF(TRIM("Attendance"), '') IS NOT NULL
  AND NULLIF(TRIM("Start_Date"), '') IS NOT NULL
