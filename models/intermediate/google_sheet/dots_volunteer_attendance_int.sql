{{ config(materialized='table') }}

WITH source_data AS (
  SELECT *
  FROM {{ source('google_sheet_staging', 'volunteer_attendance') }}
)

SELECT
  -- Identifiers
  NULLIF(TRIM("Partner_ID"), '')::integer             AS partner_id,
  NULLIF(TRIM("Volunteer_ID"), '')::integer           AS volunteer_id,
  NULLIF(TRIM("Slot_Class_Section_ID"), '')           AS slot_class_section_id,

  -- Names
  NULLIF(TRIM("Partner_Name"), '')                    AS partner_name,
  NULLIF(TRIM("volunteers"), '')                      AS volunteers,

  -- Attendance info
  NULLIF(TRIM("Attendance"), '')                      AS attendance,
  NULLIF(TRIM("Day_Of_Week"), '')                     AS day_of_week,
  NULLIF(TRIM("Current_Week"), '')                    AS current_week,
  NULLIF(TRIM("Survey_Cycle"), '')                    AS survey_cycle,
  NULLIF(TRIM("feedback"), '')                        AS feedback,

  -- Dates
  CASE
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("Start_Date"), 10), 'DD-MM-YYYY')
    WHEN NULLIF(TRIM("Start_Date"), '') ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("Start_Date"), 10)::date
    ELSE NULL
  END                                                 AS start_date,
  CASE
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{2}-\d{2}-\d{4}'
    THEN TO_DATE(LEFT(TRIM("End_Date"), 10), 'DD-MM-YYYY')
    WHEN NULLIF(TRIM("End_Date"), '') ~ '^\d{4}-\d{2}-\d{2}'
    THEN LEFT(TRIM("End_Date"), 10)::date
    ELSE NULL
  END                                                 AS end_date,
  CASE
    WHEN TRIM("Entry_Started_Date") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Started_Date")::timestamp
    ELSE NULL
  END                                                 AS entry_started_date,
  CASE
    WHEN TRIM("Entry_Last_Updated_Date") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("Entry_Last_Updated_Date")::timestamp
    ELSE NULL
  END                                                 AS entry_last_updated_date,

  -- Substitute info
  NULLIF(TRIM("Substitute_Volunteer_Name"), '')       AS substitute_volunteer_name,
  NULLIF(TRIM("Substitute_Volunteer_MAD_ID"), '')     AS substitute_volunteer_mad_id,
  CASE
    WHEN LOWER(TRIM("Volunteer_was_substituted_by_someone_else_")) IN ('yes', 'true', '1') THEN TRUE
    WHEN LOWER(TRIM("Volunteer_was_substituted_by_someone_else_")) IN ('no', 'false', '0') THEN FALSE
    ELSE NULL
  END                                                 AS volunteer_was_substituted,

  -- Survey answers
  NULLIF(TRIM("Q5__Reflections"), '')                 AS q5_reflections,

  -- Survey set 1
  NULLIF(TRIM("Q1_Set_1__Volunteer_score_for__I_created_a_safe__welcoming_spac"), '')  AS q1_set_1,
  NULLIF(TRIM("Q2_Set_1__I_asked_questions_that_helped_my_students_reflect_and"), '')  AS q2_set_1,
  NULLIF(TRIM("Q3_Set_1__My_students_asked_questions_regarding_their_academics"), '')  AS q3_set_1,
  NULLIF(TRIM("Q4_Set_1__My_students_demonstrated_engagement_and_eagerness_to_"), '')  AS q4_set_1,

  -- Survey set 2
  NULLIF(TRIM("Q1_Set_2__I_helped_my_students_identify_their_strengths_and_are"), '')  AS q1_set_2,
  NULLIF(TRIM("Q2_Set_2__I_brought_in_a_variety_of_exposure_pieces_for_my_stud"), '')  AS q2_set_2,
  NULLIF(TRIM("Q3_Set_2__My_students_set_improvement_goals_and_discussed_ways_"), '')  AS q3_set_2,
  NULLIF(TRIM("Q4_Set_2__My_students_supported_each_other_by_responding_to_eac"), '')  AS q4_set_2,

  -- Survey set 3
  NULLIF(TRIM("Q1_Set_3__I_created_a_space_where_students_could_discuss_their_"), '')  AS q1_set_3,
  NULLIF(TRIM("Q2_Set_3__I_asked_questions_and_brought_artifacts_that_helped_s"), '')  AS q2_set_3,
  NULLIF(TRIM("Q3_Set_3__My_students_are_focusing_on_asking_questions_about_ge"), '')  AS q3_set_3,
  NULLIF(TRIM("Q4_Set_3__My_students_report_that_they_are_being_regular_with_a"), '')  AS q4_set_3,

  -- Survey set 4
  NULLIF(TRIM("Q1_Set_4__My_students_have_attained_a_school_attendance_of_over"), '')  AS q1_set_4,
  NULLIF(TRIM("Q2_Set_4__My_students_have_regularly_completed_all_school_and_C"), '')  AS q2_set_4,
  NULLIF(TRIM("Q3_Set_4__My_students_have_achieved_their_monthly_improvement_g"), '')  AS q3_set_4,
  NULLIF(TRIM("Q4_Set_4__My_students_report_feeling_safe__happy_and_trusted_in"), '')  AS q4_set_4,

  -- Form metadata
  NULLIF(TRIM("Title"), '')                           AS form_title,
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
    WHEN TRIM("lastActivity_date") ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    THEN TRIM("lastActivity_date")::timestamp
    ELSE NULL
  END                                                 AS last_activity_date,

  -- Airbyte metadata
  _airbyte_raw_id,
  _airbyte_extracted_at,
  _airbyte_meta

FROM source_data
WHERE NULLIF(TRIM("Partner_ID"), '') IS NOT NULL
  AND NULLIF(TRIM("Volunteer_ID"), '') IS NOT NULL
  AND NULLIF(TRIM("Attendance"), '') IS NOT NULL
  AND NULLIF(TRIM("Start_Date"), '') IS NOT NULL
