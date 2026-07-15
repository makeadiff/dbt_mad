{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_class` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_class, and not
-- int_bubble__school_class) because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. int_bubble__school_class doesn't carry is_active or created_by through its
--      select list, both of which the target schema needs - resolving independently
--      here rather than modifying that shared silver model for one consumer.
--
-- FK resolution:
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner)
--     Loose/logical reference per the target schema (school_id is a plain BigIntegerField,
--     not a DB-level FK) - 0 unmatched rows in bubble_raw as of this build.
--   * school_academic_year_id -> school_academic_year._id -> school_academic_year.school_academic_year_id
--     (stg_bubble__school_academic_year) - 0 unmatched rows.
--   * class_id -> class._id -> class.class_id (stg_bubble__class)
--
-- 8 rows have a NULL class_id in bubble_raw (6 of 8 are removed=true/is_active=false;
-- 2 look "live"). class_id is NOT NULL on the target, so these rows can't load there -
-- excluded here per instruction, same policy as the null class_section_id/program_id
-- cases in the sibling migration models.
--
-- created_by/updated_by: 0 null raw Created_By, but ~24 rows carry a Created_By UUID
-- with no matching row in bubble_raw."user". Falls back to the 'admin' user (user_id
-- 477022) whenever unresolved, same rule as the other migration models. bubble tracks
-- no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw school_class_id is currently unique (397 rows / 397 distinct ids as of this
-- build) - kept the dedupe anyway for consistency/safety with the other migration
-- models.

with raw as (
    select
        "school_class_id"::bigint as school_class_id,
        "school_id" as school_uuid,
        "school_academic_year_id" as school_academic_year_uuid,
        "class_id" as class_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_class') }}
),

partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
school_academic_year_map as (
    select "_id" as uuid, school_academic_year_id
    from {{ ref('stg_bubble__school_academic_year') }}
),
class_map as (
    select "_id" as uuid, class_id
    from {{ ref('stg_bubble__class') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.school_class_id,
        partner_map.school_id,
        school_academic_year_map.school_academic_year_id,
        class_map.class_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join school_academic_year_map on raw.school_academic_year_uuid = school_academic_year_map.uuid
    left join class_map on raw.class_uuid = class_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
    where class_map.class_id is not null
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_class_id',
        order_by='updated_at desc',
       )
    }}
)

select
    school_class_id,
    school_id,
    school_academic_year_id,
    class_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
