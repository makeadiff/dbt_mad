{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `batch_child` table.
-- Sourced directly from bubble_raw (not stg_bubble__batch_child) for
-- created_at/updated_at precision — see prod_academic_year_migration.sql for why.
--
-- FK resolution (all resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * child_id            -> child._id            -> child.child_id       (stg_bubble__children)
--   * school_id           -> partner._id           -> partner.partner_id  (stg_bubble__partner)
--     Loose/logical reference per the target schema (school_id is a plain BigIntegerField,
--     not a DB-level FK), but still needs the same UUID->integer resolution.
--   * school_academic_year_id -> school_academic_year._id -> school_academic_year.school_academic_year_id
--     (stg_bubble__school_academic_year)
--
-- created_by/updated_by: ~189 rows have a null raw Created_By, and many more carry a
-- Created_By UUID with no matching row in bubble_raw."user" (deleted/unsynced users).
-- Falls back to the 'admin' user (user_id 477022) whenever unresolved, same rule as
-- prod_academic_year_migration.sql. bubble tracks no separate "modified by" actor, so
-- updated_by mirrors the resolved created_by value.
--
-- Raw batch_child_id is not unique in bubble_raw (8148 rows / 8115 distinct ids as of
-- this build) - deduplicated to one row per batch_child_id, keeping the latest by
-- Modified_Date.

with raw as (
    select
        "batch_child_id"::bigint as batch_child_id,
        "child_id" as child_uuid,
        "school_id" as school_uuid,
        "school_academic_year_id" as school_academic_year_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'batch_child') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
school_academic_year_map as (
    select "_id" as uuid, school_academic_year_id
    from {{ ref('stg_bubble__school_academic_year') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.batch_child_id,
        school_academic_year_map.school_academic_year_id,
        child_map.child_id,
        partner_map.school_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join school_academic_year_map on raw.school_academic_year_uuid = school_academic_year_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='batch_child_id',
        order_by='updated_at desc',
       )
    }}
)

select
    batch_child_id,
    school_academic_year_id,
    child_id,
    school_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
