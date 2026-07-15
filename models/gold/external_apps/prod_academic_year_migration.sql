{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `academic_year` table.
-- Sourced directly from bubble_raw (not stg_bubble__academic_year) because bronze
-- casts Created_Date/Modified_Date to ::date, discarding the time-of-day that the
-- target TIMESTAMPTZ columns (created_at/updated_at) need for migration fidelity.
--
-- Business rules applied here that have no equivalent in bubble:
--   * is_active: bubble currently has multiple rows flagged active at once, which
--     violates the target's single-active-row constraint (uniq_active_academic_year).
--     Only the row with the latest Modified_Date is kept active; all others are false.
--   * created_by/updated_by: bubble's Created_By is sometimes a backend workflow
--     identifier (e.g. 'admin_user_session-commencement_live') rather than a real
--     user._id, so it can't resolve via the normal UUID join. Falls back to the
--     'admin' user (user_id 477022) whenever the join finds no match.
--   * removed/deleted_at: bubble's academic_year table has no equivalent columns,
--     so these are hardcoded to false / null.
--   * updated_by: bubble tracks no separate "modified by" actor, so it mirrors
--     the resolved created_by value.

with raw as (
    select
        "academic_year_id"::bigint as academic_year_id,
        "label" as label,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'academic_year') }}
),

user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

resolved as (
    select
        raw.academic_year_id,
        raw.label,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id,
        row_number() over (order by raw.updated_at desc) as recency_rank
    from raw
    left join user_map on raw.created_by_uuid = user_map.uuid
)

select
    academic_year_id,
    label::varchar(20) as label,
    (recency_rank = 1) as is_active,
    false as removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from resolved
