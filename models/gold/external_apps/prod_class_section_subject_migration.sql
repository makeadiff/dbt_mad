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
-- FK resolution:
--   * class_section_id_id -> class_section._id -> class_section.class_section_id (stg_bubble__class_section)
--     31 of 1112 rows have a NULL raw class_section_id (confirmed genuinely blank in
--     bubble_raw, not a resolution failure). Investigated recovering these via
--     slot_class_section (class_section_subject_id -> class_section_id, denormalized
--     there too per prod_slot_class_section_migration.sql) - not viable, 0 of 31
--     recovered: only 4 of the 31 subject_ids even appear in slot_class_section, and
--     all 4 of those rows also have a NULL class_section_id there (same underlying gap,
--     all is_active=false/removed=false). class_section_id_id is NOT NULL on the
--     target, so these 31 are excluded here, same policy as the null-required-FK cases
--     in the sibling migration models.
--   * subject_id_id        -> subject._id        -> subject.subject_id            (stg_bubble__subject)
--     0 unmatched rows in bubble_raw as of this build.
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models in case future syncs introduce unresolved values. bubble tracks no
-- separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw class_section_subject_id is currently unique (1112 rows / 1112 distinct ids as of
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
    where class_section_map.class_section_id is not null
      and subject_map.subject_id is not null
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
