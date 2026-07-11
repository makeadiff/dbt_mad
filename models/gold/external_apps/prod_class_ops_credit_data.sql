{{ config(materialized='table') }}

-- Class Ops Credit Data: unique active volunteers/mentors for the current academic year,
-- joined to their credit data.
-- Built directly on the silver int_ models (not marts) because dim_pc_class /
-- fct_pc_class_sessions / fct_pc_credits don't carry academic_year, a joinable raw user_id,
-- or the full credit_data column set that old class_ops_credit_data.sql exposed via `cd.*`.

WITH filtered_class_ops AS (
    SELECT
        "SlotMentorId" AS volunteer_id,
        "AcademicYear" AS academic_year
    FROM {{ ref('int_pc_class_ops_master') }}
    WHERE "AcademicYear" = '2025-2026'
        AND "batchStatus" = 'Active'
        AND "SlotMentorId" IS NOT NULL
),

unique_volunteers AS (
    SELECT DISTINCT
        volunteer_id,
        academic_year
    FROM filtered_class_ops
)

SELECT
    uv.volunteer_id,
    uv.academic_year,
    cd."UserId" AS user_id,
    cd."UserRoles" AS user_roles,
    cd."CreditPoint" AS credit_point,
    cd."TaggedAtName" AS tagged_at_name,
    cd."TaggedAtType" AS tagged_at_type,
    cd."OnboardedDate" AS onboarded_date,
    cd."VolunteerName" AS volunteer_name,
    cd."CreditPointHistory" AS credit_point_history,
    cd."ParentTaggedAtName" AS parent_tagged_at_name,
    cd."ParentTaggedAtType" AS parent_tagged_at_type,
    cd."UserUpdatedDateTime" AS user_updated_date_time,
    cd."CreditPointHistoryReason" AS credit_point_history_reason
FROM unique_volunteers uv
LEFT JOIN {{ ref('int_pc_credit_data') }} cd
    ON uv.volunteer_id::integer = cd."UserId"::integer
