{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child_removal_log` table.
-- No child_removal_log.md exists in bubble_migration_model/ (unlike every other table
-- migrated so far) - target schema was provided directly as the Django model
-- definition, not a doc. Notably this model has NO created_by/updated_by fields at all
-- (every other migrated table has them), so this output has no admin-fallback logic.
--
-- Sourced directly from bubble_raw (not stg_bubble__child_removal_log, and not
-- int_bubble__child_removal_log) for created_at/updated_at precision - see
-- prod_academic_year_migration.sql for why.
--
-- child_id: the target Django FK has an explicit db_column="child_id" set, so - unlike
-- several sibling tables - there's no doubled '_id_id' column name here.
--
-- co_id/school_id: both are plain BigIntegerFields on the target (loose/logical
-- references, not DB-level FKs - co_id to users.user_id, school_id to
-- partner.partner_id), but both still need the usual UUID->integer resolution:
--   * co_id     -> user._id    -> users.user_id     (stg_bubble__user)
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner)
-- Both resolve cleanly, 0 unmatched rows in bubble_raw as of this build.
--
-- removed_reason: target choices (REMOVED_REASONS, provided directly) mapped from the
-- 7 distinct raw removal_reason values, all of which match exactly:
--   Other -> other (1760), Inactive -> inactive (52),
--   Transferred to another school -> transferred (35), Duplicate entry -> duplicate_entry (24),
--   Added to wrong school/class by mistake -> wrong_school_class (15),
--   Dropped out of school -> dropped_out (2),
--   Child no longer interested in participating -> child_declined (1).
-- No raw value maps to the 8th choice, family_declined. Falls back to 'other' for any
-- future unmapped value, matching the dominant existing category.
--
-- removed_datetime: NOT NULL on the target with no direct bubble equivalent - per
-- instruction, set to this row's own Created_Date (a removal log is presumably created
-- at the moment the removal happens).
--
-- deleted_at: no bubble equivalent and no business rule specified for this table (unlike
-- slot_class_section/slot_class_section_volunteer) - left null.
--
-- Raw child_removal_log_id has heavy duplication in bubble_raw (1889 rows / 288 distinct
-- ids as of this build, ~6.5x) - deduplicated to one row per id, keeping the latest by
-- Modified_Date, same convention as every other migration model.

with raw as (
    select
        "child_removal_log_id"::bigint as child_removal_log_id,
        "child_id" as child_uuid,
        "co_id" as co_uuid,
        "school_id" as school_uuid,
        case "removal_reason"
            when 'Other' then 'other'
            when 'Inactive' then 'inactive'
            when 'Transferred to another school' then 'transferred'
            when 'Duplicate entry' then 'duplicate_entry'
            when 'Added to wrong school/class by mistake' then 'wrong_school_class'
            when 'Dropped out of school' then 'dropped_out'
            when 'Child no longer interested in participating' then 'child_declined'
            else 'other'
        end as removed_reason,
        "other_details" as other_details,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child_removal_log') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
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
        raw.child_removal_log_id,
        child_map.child_id,
        user_map.user_id_number as co_id,
        partner_map.school_id,
        raw.removed_reason,
        raw.other_details,
        raw.created_at as removed_datetime,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map on raw.co_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_removal_log_id',
        order_by='updated_at desc',
       )
    }}
)

select
    child_removal_log_id,
    child_id,
    co_id,
    school_id,
    removed_reason,
    other_details,
    removed_datetime,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at
from deduplicated
