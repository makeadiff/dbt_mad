{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child_subject` table.
-- Sourced directly from bubble_raw (not stg_bubble__child_subject) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- Per the .md [FLAG] notes: neither FK has an explicit db_column set on the target
-- Django model, so Django appends '_id' to the already-'_id'-suffixed field name,
-- producing doubled real column names. Output columns below use those exact doubled
-- names (child_id_id, class_section_subject_id_id) per the "exact DB column names"
-- convention agreed for this migration project.
--
-- FK resolution (both resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * child_id_id                -> child._id                -> child.child_id (stg_bubble__children)
--   * class_section_subject_id_id -> class_section_subject._id -> class_section_subject.class_section_subject_id
--     (stg_bubble__class_section_subject)
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models in case future syncs introduce unresolved values. bubble tracks no
-- separate "modified by" actor, so updated_by mirrors created_by.
--
-- Reminder from the .md notes: this is a history-only record in bubble, never
-- authoritative for a child's *current* subjects - carried through as-is for migration,
-- not reinterpreted here.
--
-- Raw child_subject_id is not unique in bubble_raw (4924 rows / 4814 distinct ids as of
-- this build) - deduplicated to one row per id, keeping the latest by Modified_Date.

with raw as (
    select
        "child_subject_id"::bigint as child_subject_id,
        "child_id" as child_uuid,
        "class_section_subject_id" as class_section_subject_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child_subject') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
class_section_subject_map as (
    select "_id" as uuid, class_section_subject_id
    from {{ ref('stg_bubble__class_section_subject') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.child_subject_id,
        child_map.child_id,
        class_section_subject_map.class_section_subject_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join class_section_subject_map on raw.class_section_subject_uuid = class_section_subject_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_subject_id',
        order_by='updated_at desc',
       )
    }}
)

select
    child_subject_id,
    child_id as child_id,
    class_section_subject_id as class_section_subject_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
