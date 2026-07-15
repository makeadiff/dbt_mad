{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `slot_class_section_volunteer` table.
-- Sourced directly from bubble_raw (not stg_bubble__slot_class_section_volunteer, and
-- not int_bubble__slot_class_section_volunteer) for created_at/updated_at precision -
-- see prod_academic_year_migration.sql for why - and to compute a real deleted_at (see
-- below).
--
-- Per the .md [FLAG] notes: both FKs lack an explicit db_column on the target Django
-- model, so both get the doubled '_id_id' real column name (slot_class_section_id_id,
-- volunteer_id_id). Both resolve cleanly, 0 unmatched rows in bubble_raw as of this
-- build:
--   * slot_class_section_id_id -> slot_class_section._id -> slot_class_section.slot_class_section_id
--     (bubble_raw.slot_class_section directly, not stg - see parent_scs below)
--   * volunteer_id_id           -> user._id                -> users.user_id (stg_bubble__user)
--
-- deleted_at: ported from the 'scsv' branch of volunteer_allocation_history_e2_sessions.sql
-- (mad_dbt__old_models/intermediate_aggregation/session_ops/). bubble has no dedicated
-- deleted_at column, and this row's own modified_date is not a trustworthy end-of-
-- assignment timestamp: schema changes (e.g. adding an is_active column and backfilling
-- it) and bulk archive jobs bump modified_date on already-removed rows with no real
-- state change. Confirmed on real data: slot_class_section_volunteer_id 415 (removed,
-- created_date 2025-09-12, modified_date 2026-03-16) and 1262 (same slot_class_section
-- + volunteer, created_date 2026-01-29) - the service-layer rule "one active assignment
-- per (slot_class_section, volunteer)" means 415 must have actually ended by
-- 2026-01-29 when 1262 started, well before its modified_date of 2026-03-16.
--   * If the PARENT slot_class_section is removed, deleted_at = the parent's
--     modified_date (the assignment ended when the teaching slot itself did).
--   * Else if this row is removed or is_active=false, deleted_at = LEAST(next
--     reassignment's created_at, this row's own modified_date) - the next
--     reassignment (same slot_class_section + volunteer) is a tighter, more
--     trustworthy upper bound than a modified_date that bulk operations can inflate.
--     Falls back to modified_date alone when there's no next reassignment.
--   * Else (still active): null.
-- Both rows 415 and 1262 are migrated as separate output rows (this is reassignment
-- history, not CDC duplication) - dedup stays keyed on this table's own PK
-- (slot_class_section_volunteer_id), consistent with every other migration model, not
-- the old model's business-key dedup (which collapses rows for analytical purposes).
--
-- is_active: kept as a pure passthrough of this row's own raw is_active, per
-- instruction - NOT cascaded from the parent's removed status the way the old model's
-- recomputed is_active is. deleted_at alone carries the parent-removal signal.
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- Raw slot_class_section_volunteer_id is currently unique (1588 rows / 1588 distinct
-- ids as of this build).

with raw as (
    select
        "slot_class_section_volunteer_id"::bigint as slot_class_section_volunteer_id,
        "slot_class_section_id" as slot_class_section_uuid,
        "volunteer_id" as volunteer_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'slot_class_section_volunteer') }}
),

slot_class_section_map as (
    select "_id" as uuid, "slot_class_section_id"::bigint as slot_class_section_id
    from {{ source('bubble_raw', 'slot_class_section') }}
),
parent_scs as (
    select
        "_id" as uuid,
        "removed"::boolean as removed,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'slot_class_section') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.slot_class_section_volunteer_id,
        slot_class_section_map.slot_class_section_id,
        user_map.user_id_number as volunteer_id,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        parent_scs.removed as parent_removed,
        parent_scs.updated_at as parent_updated_at,
        coalesce(user_map2.user_id_number, 477022) as resolved_user_id
    from raw
    left join slot_class_section_map on raw.slot_class_section_uuid = slot_class_section_map.uuid
    left join parent_scs on raw.slot_class_section_uuid = parent_scs.uuid
    left join user_map on raw.volunteer_uuid = user_map.uuid
    left join user_map as user_map2 on raw.created_by_uuid = user_map2.uuid
),

with_next as (
    select
        *,
        lead(created_at) over (
            partition by slot_class_section_id, volunteer_id
            order by created_at
        ) as next_assignment_start
    from joined
)

select
    slot_class_section_volunteer_id,
    slot_class_section_id as slot_class_section_id,
    volunteer_id as volunteer_id,
    is_active,
    removed,
    case
        when parent_removed then parent_updated_at
        when removed or not is_active then
            case
                when next_assignment_start is not null
                    then least(next_assignment_start, updated_at)
                else updated_at
            end
        else cast(null as timestamptz)
    end as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from with_next
