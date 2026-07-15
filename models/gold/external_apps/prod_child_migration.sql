{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `child` table.
-- Sourced directly from bubble_raw (not stg_bubble__children) for two reasons:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. dob/date_of_enrollment need a timezone fix - see below.
--
-- Per the .md M7 notes: post-M6, class enrollment lives on child_class /
-- child_class_section, not on Child - class_id and school_class_id are intentionally
-- NOT carried into this output even though bubble_raw.child still has them.
-- guardian_name/guardian_phone/admission_number (dropped in the target's migration 0012)
-- don't exist in bubble_raw.child either, so there's nothing to drop.
--
-- gender mapping: bubble_raw uses 'Male'/'Female'/'Others' (2859/2449/2 rows); target
-- only allows male/female/other. Lowercased, with Others -> other.
--
-- dob/date_of_enrollment timezone fix: every non-null value in both columns has exactly
-- '18:30:00' as its UTC time component (839/839 and 264/264 rows) - the UTC
-- representation of IST midnight (00:00 IST = 18:30 UTC the prior day). A direct ::date
-- cast (what stg_bubble__children.sql currently does) reads the UTC calendar date, one
-- day earlier than what was actually entered in Bubble. Converted to Asia/Kolkata before
-- taking the date here so the migrated value matches the real entered date.
--
-- FK resolution: school_id -> partner._id -> partner.partner_id (stg_bubble__partner).
-- Loose/logical reference per the target schema (school_id is a plain BigIntegerField,
-- not a DB-level FK) - 0 unmatched rows in bubble_raw as of this build.
--
-- created_by/updated_by: ~189 rows have a null raw Created_By, and ~103 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models.
-- bubble tracks no separate "modified by" actor, so updated_by mirrors created_by.
--
-- mad_joining_date: no bubble equivalent - left null (nullable on the target).
--
-- Raw child_id is currently unique (5310 rows / 5310 distinct ids as of this build).

with raw as (
    select
        "child_id"::bigint as child_id,
        "school_id" as school_uuid,
        "first_name" as first_name,
        "last_name" as last_name,
        case lower("gender")
            when 'male' then 'male'
            when 'female' then 'female'
            when 'others' then 'other'
            else lower("gender")
        end as gender,
        (("dob"::timestamptz) at time zone 'Asia/Kolkata')::date as date_of_birth,
        "Age"::integer as age,
        "city" as city,
        "mother_tounge" as mother_tongue,
        (("date_of_enrollment"::timestamptz) at time zone 'Asia/Kolkata')::date as date_of_enrollment,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'child') }}
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
        raw.child_id,
        partner_map.school_id,
        raw.first_name,
        raw.last_name,
        raw.gender,
        raw.date_of_birth,
        raw.age,
        raw.city,
        raw.mother_tongue,
        raw.date_of_enrollment,
        raw.is_active,
        raw.removed,
        raw.created_at,
        raw.updated_at,
        coalesce(user_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map on raw.created_by_uuid = user_map.uuid
)

select
    child_id,
    school_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    age,
    city,
    mother_tongue,
    date_of_enrollment,
    cast(null as date) as mad_joining_date,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from joined
