{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `class_section` table.
-- Sourced directly from bubble_raw (not stg_bubble__class_section) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- section_display_name: no source column in bubble - per instruction, set equal to
-- section_name (matches the target's own M6 backfill history: section_display_name
-- was originally backfilled from section_name).
--
-- section_code: 10 rows use 'M'/'N', outside the target's documented A-L choices.
-- No DB-level CHECK constraint exists on this column (just VARCHAR(1) + app-level
-- choices), so passed through as-is per instruction rather than nulled out.
--
-- FK resolution:
--   * school_class_id -> school_class._id -> school_class.school_class_id (stg_bubble__school_class)
--     Nullable on the target, but 0 nulls/unresolved in bubble_raw as of this build.
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner)
--     Loose/logical reference per the target schema (school_id is a plain BigIntegerField,
--     not a DB-level FK) - 0 unmatched rows in bubble_raw as of this build.
--
-- created_by/updated_by: 8 rows have a null raw Created_By, and ~148 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models.
-- bubble tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw class_section_id is currently unique (1852 rows / 1852 distinct ids as of this
-- build).

with raw as (
    select
        "class_section_id"::bigint as class_section_id,
        "school_class_id" as school_class_uuid,
        "school_id" as school_uuid,
        "section_code" as section_code,
        "section_name" as section_name,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'class_section') }}
),

school_class_map as (
    select "_id" as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
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
        raw.class_section_id,
        school_class_map.school_class_id,
        partner_map.school_id,
        raw.section_code,
        raw.section_name,
        raw.section_name as section_display_name,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join school_class_map on raw.school_class_uuid = school_class_map.uuid
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
)

select
    class_section_id,
    school_class_id,
    school_id,
    section_code,
    section_name,
    section_display_name,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from joined
