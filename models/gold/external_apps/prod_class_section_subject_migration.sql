{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `class_section_subject` table.
-- Sourced directly from bubble_raw (not stg_bubble__class_section_subject) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- Per the .md [FLAG] notes: neither FK has an explicit db_column set on the target
-- Django model, so both get the doubled '_id_id' real column name. Output columns
-- below use those exact doubled names (class_section_id_id, subject_id_id) per the
-- "exact DB column names" convention agreed for this migration project.
--
-- FK resolution (both resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * class_section_id_id -> class_section._id -> class_section.class_section_id (stg_bubble__class_section)
--   * subject_id_id        -> subject._id        -> subject.subject_id            (stg_bubble__subject)
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models in case future syncs introduce unresolved values. bubble tracks no
-- separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw class_section_subject_id is currently unique (1087 rows / 1087 distinct ids as of
-- this build).

with raw as (
    select
        "class_section_subject_id"::bigint as class_section_subject_id,
        "class_section_id" as class_section_uuid,
        "subject_id" as subject_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'class_section_subject') }}
),

class_section_map as (
    select "_id" as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),
subject_map as (
    select "_id" as uuid, subject_id
    from {{ ref('stg_bubble__subject') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.class_section_subject_id,
        class_section_map.class_section_id,
        subject_map.subject_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join class_section_map on raw.class_section_uuid = class_section_map.uuid
    left join subject_map on raw.subject_uuid = subject_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
)

select
    class_section_subject_id,
    class_section_id as class_section_id,
    subject_id as subject_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from joined
