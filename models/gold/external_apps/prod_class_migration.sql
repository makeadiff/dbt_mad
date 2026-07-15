{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `class` table (global class/grade catalog).
-- Sourced directly from bubble_raw (not stg_bubble__class) for two reasons:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. program_id is a plain numeric column in bubble_raw.class (not a UUID, unlike
--      program_id on child_program) - it already points directly at program.program_id,
--      so no UUID join is needed or possible here; passed through as-is.
--
-- is_active/removed: bubble_raw.class has neither column at all (only 9 columns total -
-- confirmed against the live raw table, not just the stg model). Hardcoded to
-- true/false here, matching the target schema's own column defaults.
--
-- class_code: numeric in bubble_raw (5,6,7,8...) but VARCHAR(4) on the target - cast to
-- text.
--
-- created_by/updated_by: all 4 current rows have Created_By set to a backend workflow
-- identifier ('admin_user_session-commencement_test'/'_live'), same pattern as
-- prod_academic_year_migration.sql, not a real user._id. Falls back to the 'admin' user
-- (user_id 477022) for all of them. created_by is nullable on this target (unlike most
-- other migrated tables) but the admin fallback is applied anyway for consistency across
-- all migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- Raw class_id is currently unique (4 rows / 4 distinct ids as of this build) - kept the
-- dedupe anyway for consistency/safety with the other migration models.

with raw as (
    select
        "class_id"::bigint as class_id,
        "class_name" as class_name,
        "class_code"::varchar(4) as class_code,
        "program_id"::bigint as program_id,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'class') }}
),

user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.class_id,
        raw.class_name,
        raw.class_code,
        raw.program_id,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='class_id',
        order_by='updated_at desc',
       )
    }}
)

select
    class_id,
    class_name,
    class_code,
    program_id,
    true as is_active,
    false as removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
