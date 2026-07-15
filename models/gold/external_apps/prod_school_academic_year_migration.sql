{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_academic_year` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_academic_year, and not
-- int_bubble__school_academic_year) because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. int_bubble__school_academic_year carries created_by as an unresolved raw UUID,
--      not the resolved integer user_id the target needs - resolving independently here.
--
-- FK resolution:
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner)
--     Raw integer per the target schema (not a DB-level FK, service-layer validated
--     against partner.partner_id per the .md notes) - 0 unmatched rows in bubble_raw.
--   * academic_year_id -> academic_year._id -> academic_year.academic_year_id
--     (stg_bubble__academic_year) - 0 unmatched rows.
--
-- Checked the uniq_school_academic_year constraint (school_id, academic_year_id) where
-- removed=false against current data: no violations found.
--
-- created_by/updated_by: 0 null raw Created_By, but 16 rows carry a Created_By UUID
-- with no matching row in bubble_raw."user". Falls back to the 'admin' user (user_id
-- 477022) whenever unresolved, same rule as the other migration models. bubble tracks
-- no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw school_academic_year_id is currently unique (176 rows / 176 distinct ids as of
-- this build) - kept the dedupe anyway for consistency/safety with the other migration
-- models.
--
-- Per the .md M7 notes: this model must be loaded into the target *after*
-- prod_academic_year_migration.sql specifically - it's a hard FK dependency for
-- school_class, school_session_details, and slot.

with raw as (
    select
        "school_academic_year_id"::bigint as school_academic_year_id,
        "school_id" as school_uuid,
        "academic_year_id" as academic_year_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_academic_year') }}
),

partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
academic_year_map as (
    select "_id" as uuid, academic_year_id
    from {{ ref('stg_bubble__academic_year') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.school_academic_year_id,
        partner_map.school_id,
        academic_year_map.academic_year_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join academic_year_map on raw.academic_year_uuid = academic_year_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_academic_year_id',
        order_by='updated_at desc',
       )
    }}
)

select
    school_academic_year_id,
    school_id,
    academic_year_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
