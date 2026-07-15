{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_session_details` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_session_detail) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why - and
-- because start_date/end_date get the same IST timezone fix as dob/date_of_enrollment
-- in prod_child_migration.sql. Here the raw time component is mixed (2/3 end_date rows
-- at 00:00:00 UTC, the rest at the usual '18:30:00' UTC-midnight-IST signature); the
-- Asia/Kolkata conversion is a no-op for the 00:00:00 UTC rows (same calendar day either
-- way) and corrects the 18:30:00 ones, so applying it uniformly is safe.
--
-- Per the .md notes: the source field is named school_academic_year (no _id suffix), so
-- Django assigns the plain db_column school_academic_year_id - no doubled-suffix gotcha
-- here, unlike several sibling tables.
--
-- FK resolution (both resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner). Loose/logical
--     reference per the target schema (school_id is a plain BigIntegerField, not a
--     DB-level FK).
--   * school_academic_year_id -> school_academic_year._id -> school_academic_year.school_academic_year_id
--     (stg_bubble__school_academic_year)
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- Raw session_id is currently unique (3 rows / 3 distinct ids as of this build) - kept
-- the dedupe anyway for consistency/safety with the other migration models.

with raw as (
    select
        "session_id"::bigint as session_id,
        "school_id" as school_uuid,
        "school_academic_year" as school_academic_year_uuid,
        (("start_date"::timestamptz) at time zone 'Asia/Kolkata')::date as start_date,
        (("end_date"::timestamptz) at time zone 'Asia/Kolkata')::date as end_date,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_session_detail') }}
),

partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
school_academic_year_map as (
    select "_id" as uuid, school_academic_year_id
    from {{ ref('stg_bubble__school_academic_year') }}
),
user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.session_id,
        partner_map.school_id,
        school_academic_year_map.school_academic_year_id,
        raw.start_date,
        raw.end_date,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join school_academic_year_map on raw.school_academic_year_uuid = school_academic_year_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='session_id',
        order_by='updated_at desc',
       )
    }}
)

select
    session_id,
    school_id,
    school_academic_year_id,
    start_date,
    end_date,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
