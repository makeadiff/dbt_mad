{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `subject` table (global subject catalog, e.g.
-- 'Foundation Day 1'). Sourced directly from bubble_raw (not stg_bubble__subject) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- Target schema note (sessionops/models/subject.py): Subject has no is_active/removed/
-- deleted_at at all - like Class and Program, it's a "universal catalog... never
-- deactivated" (same doc language as Class). So there's nothing to derive from bubble's
-- own "removed" column: it's dropped here, not mapped to anything. All 3 rows are migrated
-- unconditionally regardless of bubble's removed flag (1 of the 3 - "Life Skills" - is
-- removed=true in bubble as of this build), same as prod_class_migration.sql/
-- prod_program_migration.sql never filtering on a lifecycle column the target doesn't have.
-- Dropping a removed-in-bubble row here instead would break the subject_id FK on any
-- already-migrated prod_child_subject_migration/prod_class_section_subject_migration row
-- that still points at it.
--
-- program_id: a UUID in bubble_raw.subject (unlike class.program_id, which is already
-- plain numeric) - resolved via stg_bubble__program to the same program_id space
-- prod_program_migration.sql outputs. All 3 rows resolve cleanly to program_id 1 as of
-- this build.
--
-- created_by/updated_by: all 3 rows have Created_By set to the same synthetic
-- 'admin_user_session-commencement_...' workflow identifier seen on class/program/
-- academic_year, not a real user._id. Falls back to the 'admin' user (user_id 477022),
-- same rule as every other migration model. bubble tracks no separate "modified by" actor,
-- so updated_by mirrors created_by.
--
-- Raw subject_id and subject_name are both currently unique (3 rows / 3 distinct ids and
-- names as of this build) - kept the dedupe anyway for consistency/safety with the other
-- migration models.

with raw as (
    select
        "subject_id"::bigint as subject_id,
        "subject_name" as subject_name,
        "program_id" as program_uuid,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'subject') }}
),

program_map as (
    select "_id" as uuid, program_id
    from {{ ref('stg_bubble__program') }}
),

user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.subject_id,
        raw.subject_name,
        program_map.program_id,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join program_map on raw.program_uuid = program_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='subject_id',
        order_by='updated_at desc',
       )
    }}
)

select
    subject_id,
    subject_name,
    program_id,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
