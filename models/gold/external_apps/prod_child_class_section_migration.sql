{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child_class_section` table.
-- Sourced directly from bubble_raw (not stg_bubble__child_class_section, and not
-- int_bubble__child_class_section) because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. int_bubble__child_class_section doesn't carry is_active or created_by through
--      its select list, both of which the target schema needs - resolving independently
--      here rather than modifying that shared silver model for one consumer.
--
-- FK resolution:
--   * child_id         -> child._id         -> child.child_id         (stg_bubble__children)
--   * class_section_id -> class_section._id -> class_section.class_section_id (stg_bubble__class_section)
--
-- 47 rows in bubble_raw have a NULL class_section_id (21 of them otherwise look "live":
-- is_active=true, removed=false). class_section_id is NOT NULL on the target, so these
-- rows can't load there - excluded here rather than migrated with a broken FK.
--
-- created_by/updated_by: ~195 rows have a null raw Created_By, and ~836 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models.
-- bubble tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw child_class_section_id is not unique in bubble_raw (8655 rows / 8619 distinct ids
-- as of this build) - deduplicated to one row per id, keeping the latest by Modified_Date.

with raw as (
    select
        "child_class_section_id"::bigint as child_class_section_id,
        "child_id" as child_uuid,
        "class_section_id" as class_section_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child_class_section') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
class_section_map as (
    select "_id" as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.child_class_section_id,
        child_map.child_id,
        class_section_map.class_section_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join class_section_map on raw.class_section_uuid = class_section_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
    where class_section_map.class_section_id is not null
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_class_section_id',
        order_by='updated_at desc',
       )
    }}
)

select
    child_class_section_id,
    child_id,
    class_section_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
