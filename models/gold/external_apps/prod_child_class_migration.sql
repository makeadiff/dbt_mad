{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child_class` table.
-- Sourced directly from bubble_raw (not stg_bubble__child_class) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- FK resolution (both resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * child_id        -> child._id        -> child.child_id        (stg_bubble__children)
--   * school_class_id -> school_class._id -> school_class.school_class_id (stg_bubble__school_class)
--
-- Per the .md notes: bubble/target both enforce "one active child_class per child" only
-- at the service layer, not the DB - no UniqueConstraint exists on either side. Checked
-- current data for violations (multiple is_active=true/removed=false rows per child_id):
-- none found as of this build, so no cleanup pass is needed here. The invariant itself
-- is a service-layer concern for the new platform, not something this model can enforce.
--
-- created_by/updated_by: ~189 rows have a null raw Created_By, and ~918 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models.
-- bubble tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw child_class_id is not unique in bubble_raw (8190 rows / 8155 distinct ids as of
-- this build) - deduplicated to one row per id, keeping the latest by Modified_Date.

with raw as (
    select
        "child_class_id"::bigint as child_class_id,
        "child_id" as child_uuid,
        "school_class_id" as school_class_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child_class') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
school_class_map as (
    select "_id" as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.child_class_id,
        child_map.child_id,
        school_class_map.school_class_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join school_class_map on raw.school_class_uuid = school_class_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_class_id',
        order_by='updated_at desc',
       )
    }}
)

select
    child_class_id,
    child_id,
    school_class_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
