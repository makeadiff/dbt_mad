{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `slot_class_section` table.
-- Sourced directly from bubble_raw (not stg_bubble__slot_class_section) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- Per the .md [FLAG] notes: all three FKs lack an explicit db_column on the target
-- Django model, so all three get the doubled '_id_id' real column name
-- (slot_id_id, class_section_id_id, class_section_subject_id_id). Per the .md's own
-- notes this is a deliberate Bubble-schema-driven denormalization (class_section_id and
-- class_section_subject_id both stored even though the latter implies the former) -
-- carried through as-is, not deduplicated away.
--
-- FK resolution (all three resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * slot_id_id                  -> slot._id                  -> slot.slot_id (stg_bubble__slot)
--   * class_section_id_id         -> class_section._id         -> class_section.class_section_id (stg_bubble__class_section)
--   * class_section_subject_id_id -> class_section_subject._id -> class_section_subject.class_section_subject_id
--     (stg_bubble__class_section_subject)
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- deleted_at: bubble has no dedicated deleted_at column, so it's derived rather than
-- hardcoded null, per the 'scs' branch of the end-date logic in the old
-- volunteer_allocation_history_e2_sessions.sql model (mad_dbt__old_models/
-- intermediate_aggregation/session_ops/) - `WHEN scs.removed = TRUE THEN
-- scs.modified_date`. When removed=true, deleted_at = updated_at (our resolved
-- full-precision Modified_Date); otherwise null. Unlike that old model's fuller 'scsv'
-- branch (used for slot_class_section_volunteer), no LEAD/LEAST capping against a next
-- reassignment applies here - that logic is specific to volunteer reassignment history,
-- not this table.
--
-- Raw slot_class_section_id is not unique in bubble_raw (1087 rows / 1085 distinct ids
-- as of this build) - deduplicated to one row per id, keeping the latest by
-- Modified_Date.

with raw as (
    select
        "slot_class_section_id"::bigint as slot_class_section_id,
        "slot_id" as slot_uuid,
        "class_section_id" as class_section_uuid,
        "class_section_subject_id" as class_section_subject_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'slot_class_section') }}
),

slot_map as (
    select "_id" as uuid, slot_id
    from {{ ref('stg_bubble__slot') }}
),
class_section_map as (
    select "_id" as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
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
        raw.slot_class_section_id,
        slot_map.slot_id,
        class_section_map.class_section_id,
        class_section_subject_map.class_section_subject_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join slot_map on raw.slot_uuid = slot_map.uuid
    left join class_section_map on raw.class_section_uuid = class_section_map.uuid
    left join class_section_subject_map on raw.class_section_subject_uuid = class_section_subject_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='slot_class_section_id',
        order_by='updated_at desc',
       )
    }}
)

select
    slot_class_section_id,
    slot_id as slot_id,
    class_section_id as class_section_id,
    class_section_subject_id as class_section_subject_id,
    is_active,
    removed,
    case when removed then updated_at else cast(null as timestamptz) end as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
