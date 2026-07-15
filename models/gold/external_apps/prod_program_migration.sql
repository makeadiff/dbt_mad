{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `program` table (layer-0 reference
-- catalog, e.g. 'Education Support'). Sourced directly from bubble_raw (not
-- stg_bubble__program) for created_at/updated_at precision - see
-- prod_academic_year_migration.sql for why.
--
-- is_active/removed: bubble_raw.program has neither column at all (confirmed against
-- the live raw table). Hardcoded to true/false here, matching the target schema's own
-- column defaults. Matches the .md's own note that this catalog is "effectively never
-- deactivated in practice" anyway.
--
-- created_by/updated_by: the single current row's Created_By is the same synthetic
-- 'admin_user_session-commencement_test' workflow identifier seen on class/academic_year,
-- not a real user._id. Falls back to the 'admin' user (user_id 477022), same rule as the
-- other migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- Raw program_id is currently unique (1 row / 1 distinct id as of this build) - kept the
-- dedupe anyway for consistency/safety with the other migration models.

with raw as (
    select
        "program_id"::bigint as program_id,
        "program_name" as program_name,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'program') }}
),

user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.program_id,
        raw.program_name,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='program_id',
        order_by='updated_at desc',
       )
    }}
)

select
    program_id,
    program_name,
    true as is_active,
    false as removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
