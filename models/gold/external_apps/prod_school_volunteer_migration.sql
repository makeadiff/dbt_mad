{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_volunteer` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_volunteer, and not
-- int_bubble__school_volunteer) because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. int_bubble__school_volunteer doesn't carry is_active or created_by through its
--      select list, both of which the target schema needs.
--
-- Per the .md [FLAG] notes: neither volunteer_id nor school_academic_year_id has an
-- explicit db_column set on the target Django model, so both get the doubled '_id_id'
-- real column name (volunteer_id_id, school_academic_year_id_id).
--
-- school_id: 3894 of 5500 rows (71%) have a NULL school_id in bubble_raw - including
-- 3868 rows that are is_active=true/removed=false (93% of all "active" rows), almost
-- all bulk-created on a single day (2026-06-16), suggesting a batch operation in bubble
-- that didn't set school_id. Investigated a proposed recovery path
-- (school_volunteer.volunteer_id -> user._id -> user.worknode_id ->
-- prod_chapter_mapping.worknode_id -> chapter_mapping.chapter_id -> bubble
-- chapter.chapter_id -> chapter_school.school_id -> partner.partner_id): it only
-- recovered 2 of 3894 rows, because chapter_mapping's chapter numbering (CRM/Google
-- Sheet roster, all of MAD's chapters) doesn't correspond to bubble_raw.chapter's own
-- numbering (session-ops-only, much smaller). Not viable - excluded here per
-- instruction, same policy as the null-required-FK cases in the sibling migration
-- models.
--
-- school_academic_year_id_id: no source column at all in bubble_raw.school_volunteer
-- (confirmed against the live raw table) - left null (nullable on the target, and the
-- .md notes this FK was made nullable in migration 0023 for this reason).
--
-- volunteer_id_id: 3 rows have a NULL raw volunteer_id. volunteer_id_id is NOT NULL on
-- the target, so these are excluded too (on top of the school_id exclusions above).
--
-- created_by/updated_by: 8 rows have a null raw Created_By, and ~51 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models,
-- even though created_by is nullable on this target (unlike most other tables) - kept
-- for consistency across all migration models. bubble tracks no separate "modified by"
-- actor, so updated_by mirrors created_by.
--
-- Raw school_volunteer_id is not unique in bubble_raw (5500 rows / 5496 distinct ids as
-- of this build) - deduplicated to one row per id, keeping the latest by Modified_Date.

with raw as (
    select
        "school_volunteer_id"::bigint as school_volunteer_id,
        "school_id" as school_uuid,
        "volunteer_id" as volunteer_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_volunteer') }}
),

partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.school_volunteer_id,
        partner_map.school_id,
        volunteer_map.user_id_number as volunteer_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(created_by_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map as volunteer_map on raw.volunteer_uuid = volunteer_map.uuid
    left join user_map as created_by_map on raw.created_by_uuid = created_by_map.uuid
    where partner_map.school_id is not null
      and volunteer_map.user_id_number is not null
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_volunteer_id',
        order_by='updated_at desc',
       )
    }}
)

select
    school_volunteer_id,
    school_id,
    volunteer_id as volunteer_id,
    cast(null as bigint) as school_academic_year_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
