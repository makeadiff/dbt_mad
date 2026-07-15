{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child_program` table.
-- Sourced directly from bubble_raw (not stg_bubble__child_program) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why.
--
-- FK resolution:
--   * child_id   -> child._id   -> child.child_id     (stg_bubble__children) - 0 unmatched.
--   * program_id -> program._id -> program.program_id (stg_bubble__program)
--
-- 33 rows have a NULL program_id in bubble_raw (all 33 unresolved rows are exactly the
-- null ones - no non-null UUID fails to match). program_id is NOT NULL on the target,
-- so per instruction these rows fall back to program_id 1 ('Education Support') rather
-- than being excluded.
--
-- is_active: bubble_raw.child_program has no is_active column at all (confirmed against
-- the live raw table, not just the stg model - this isn't a dropped column). Hardcoded
-- to true here, matching the target schema's own column default.
--
-- created_by/updated_by: ~189 rows have a null raw Created_By, and ~103 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models.
-- bubble tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw child_program_id is currently unique (5310 rows / 5310 distinct ids as of this
-- build) but deduplicated anyway for safety, keeping the latest by Modified_Date -
-- every sibling bubble raw table checked so far (batch_child, child_class_section,
-- child_class) has turned out to carry duplicate ids from Airbyte re-syncs.

with raw as (
    select
        "child_program_id"::bigint as child_program_id,
        "child_id" as child_uuid,
        "program_id" as program_uuid,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child_program') }}
),

child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
program_map as (
    select "_id" as uuid, program_id
    from {{ ref('stg_bubble__program') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.child_program_id,
        child_map.child_id,
        coalesce(program_map.program_id, 1) as program_id,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join child_map on raw.child_uuid = child_map.uuid
    left join program_map on raw.program_uuid = program_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_program_id',
        order_by='updated_at desc',
       )
    }}
)

select
    child_program_id,
    program_id,
    child_id,
    true as is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
