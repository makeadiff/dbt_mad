{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `slot` table.
-- Sourced directly from bubble_raw (not stg_bubble__slot, and not int_bubble__slot)
-- because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. start_time/end_time need the same IST timezone fix as dob/date_of_enrollment in
--      prod_child_migration.sql: raw values are full UTC timestamps (arbitrary dates,
--      only the time-of-day matters for the target's TIME columns), entered as IST
--      wall-clock time. Converted to Asia/Kolkata before taking ::time here so the
--      migrated time matches what was actually entered.
--   3. int_bubble__slot doesn't carry created_by through, and stg_bubble__slot casts
--      start_time/end_time to plain ::timestamp (dropping the timezone info needed for
--      the fix above).
--
-- day_of_week: raw values are capitalized ('Tuesday') vs the target's lowercase choices
-- (monday..sunday) - lowercased. 3 rows have a NULL raw day_of_week; rather than exclude
-- them, derived the day name from start_time itself (same IST-converted timestamp,
-- to_char(..., 'Day')) since the information already exists in the same row.
--
-- Per the .md [FLAG] notes: school_academic_year_id has no explicit db_column set on
-- the target Django model, so it gets the doubled 'school_academic_year_id_id' real
-- column name.
--
-- recurring: sourced from bubble_raw's "reccuring" column (note the raw typo).
--
-- FK resolution (both resolve cleanly, 0 unmatched rows in bubble_raw as of this build):
--   * school_id -> partner._id -> partner.partner_id (stg_bubble__partner). Loose/logical
--     reference per the target schema (school_id is a plain BigIntegerField, not a
--     DB-level FK).
--   * school_academic_year_id_id -> school_academic_year._id -> school_academic_year.school_academic_year_id
--     (stg_bubble__school_academic_year)
--
-- created_by/updated_by: 1 row has a null raw Created_By, and ~4 more carry a Created_By
-- UUID with no matching row in bubble_raw."user". Falls back to the 'admin' user
-- (user_id 477022) whenever unresolved, same rule as the other migration models. bubble
-- tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- Raw slot_id is currently unique (178 rows / 178 distinct ids as of this build) - kept
-- the dedupe anyway for consistency/safety with the other migration models.

with raw as (
    select
        "slot_id"::bigint as slot_id,
        "school_id" as school_uuid,
        "school_academic_year_id" as school_academic_year_uuid,
        "slot_name" as slot_name,
        coalesce(
            lower("day_of_week"),
            lower(trim(to_char((("start_time"::timestamptz)) at time zone 'Asia/Kolkata', 'Day')))
        ) as day_of_week,
        date_trunc('second', ("start_time"::timestamptz) at time zone 'Asia/Kolkata')::time without time zone as start_time,
        date_trunc('second', ("end_time"::timestamptz) at time zone 'Asia/Kolkata')::time without time zone as end_time,
        "reccuring"::boolean as recurring,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'slot') }}
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
        raw.slot_id,
        partner_map.school_id,
        school_academic_year_map.school_academic_year_id,
        raw.slot_name,
        raw.day_of_week,
        raw.start_time,
        raw.end_time,
        raw.recurring,
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
        partition_by='slot_id',
        order_by='updated_at desc',
       )
    }}
)

select
    slot_id,
    school_id,
    school_academic_year_id as school_academic_year_id,
    slot_name,
    day_of_week,
    start_time,
    end_time,
    recurring,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
