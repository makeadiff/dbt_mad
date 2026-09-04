{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_holiday` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_holiday) for
-- created_at/updated_at precision - see prod_academic_year_migration.sql for why - and
-- because start_date/end_date need the same IST timezone fix as dob/date_of_enrollment
-- in prod_child_migration.sql (every value carries the '18:30:00' UTC-midnight-IST
-- signature; a direct ::date cast would be one day early).
--
-- holiday_reason: bubble_raw only has 2 rows today, with free-text-ish values
-- ('MAD event (eg: YEC, etc)', 'Cancelled from school's end' - the raw apostrophe is a
-- mangled encoding artifact) against the target's fixed choices (mad_event / holidays /
-- cancelled_from_school_end). Mapped by prefix match per instruction; anything that
-- doesn't match either prefix falls back to 'holidays' (the one choice with no current
-- source value) so the column always gets a valid, non-null choice.
--
-- remarks: no source column in bubble_raw.school_holiday at all (confirmed against the
-- live raw table) - left null (nullable on the target).
--
-- FK resolution: school_id -> partner._id -> partner.partner_id (stg_bubble__partner).
-- Loose/logical reference per the target schema (school_id is a plain BigIntegerField,
-- not a DB-level FK) - 0 unmatched rows in bubble_raw as of this build.
--
-- created_by/updated_by: 0 null/unresolved Created_By rows in bubble_raw as of this
-- build, but the admin fallback (user_id 477022) is kept for parity with the other
-- migration models. bubble tracks no separate "modified by" actor, so updated_by
-- mirrors created_by.
--
-- school_holiday_id is NOT a reliable unique key in bubble_raw - same bug class found in
-- prod_child_removal_log_migration and int_bubble__school_holiday (see either header for
-- the full writeup; int_bubble__school_holiday was actually the first place this pattern
-- was caught). Every raw row is already genuinely distinct (0 true re-sync duplication on
-- "_id"); the repetition is Bubble's own school_holiday_id counter assigning the same
-- number to holiday records for different schools. As of this build: 3 raw rows / 1
-- distinct id (all 3 for different schools) - deduplicating on school_holiday_id was
-- silently dropping 2 of the 3 real holiday records.
--
-- Since school_holiday_id is this table's target PK, deduplicated on raw "_id" instead
-- (the true unique Bubble row, defensive - no actual dupes found as of this build) and
-- reassigned school_holiday_id as a fresh sequential integer ordered by (created_at,
-- _id) so every real record gets its own PK. The original bubble value is kept in
-- bubble_school_holiday_id for traceability - it will legitimately repeat across rows.

with raw as (
    select
        "_id" as raw_uid,
        "school_holiday_id"::bigint as bubble_school_holiday_id,
        "school_id" as school_uuid,
        case
            when "holiday_reason" ilike 'MAD event%' then 'mad_event'
            when "holiday_reason" ilike 'Cancelled from school%' then 'cancelled_from_school_end'
            else 'holidays'
        end as holiday_reason,
        (("start_date"::timestamptz) at time zone 'Asia/Kolkata')::date as start_date,
        (("end_date"::timestamptz) at time zone 'Asia/Kolkata')::date as end_date,
        "holiday_description" as holiday_description,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_holiday') }}
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
        raw.raw_uid,
        raw.bubble_school_holiday_id,
        partner_map.school_id,
        raw.holiday_reason,
        raw.start_date,
        raw.end_date,
        raw.holiday_description,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='raw_uid',
        order_by='updated_at desc',
       )
    }}
)

select
    row_number() over (order by created_at, raw_uid) as school_holiday_id,
    bubble_school_holiday_id,
    school_id,
    holiday_reason,
    start_date,
    end_date,
    holiday_description,
    cast(null as text) as remarks,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
