{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `school_volunteer` table.
-- Sourced directly from bubble_raw (not stg_bubble__school_volunteer, and not
-- int_bubble__school_volunteer) because:
--   1. created_at/updated_at need full timestamp precision - see
--      prod_academic_year_migration.sql for why bronze's ::date cast is unusable here.
--   2. int_bubble__school_volunteer doesn't carry is_active or created_by through its
--      select list, both of which the target schema needs.
--
-- Per the .md [FLAG] notes: neither volunteer_id nor school_academic_year_id has an
-- explicit db_column set on the target Django model, so both get the doubled '_id_id'
-- real column name (volunteer_id_id, school_academic_year_id_id).
--
-- school_id: 3894 of 5500 rows (71%) have a NULL school_id in bubble_raw - including
-- 3868 rows that are is_active=true/removed=false (93% of all "active" rows), almost
-- all bulk-created on a single day (2026-06-16), suggesting a batch operation in bubble
-- that didn't set school_id. A first recovery path (school_volunteer.volunteer_id ->
-- user._id -> user.worknode_id -> prod_chapter_mapping.worknode_id ->
-- chapter_mapping.chapter_id -> bubble chapter.chapter_id -> chapter_school.school_id
-- -> partner.partner_id) only recovered 2 of 3894 rows, because that path's
-- chapter_mapping hop lands on bubble_raw.chapter's own numbering (session-ops-only,
-- much smaller), not the same id space as school_id.
--
-- A second, shorter path recovers 68 more: volunteer_id -> user.worknode_id ->
-- prod_chapter_mapping.worknode_id (engine = 'E2' only). For E2, prod_chapter_mapping's
-- chapter_id is validated in int_google_sheet__chapter_mapping against
-- int_crm__partners.partner_id (see chapter_validation there) - the same CRM-sourced id
-- space as stg_bubble__partner.partner_id1/school_id here, unlike the first path's
-- bubble-internal chapter numbering. Confirmed by direct query against prod: all 68
-- recovered chapter_id values match a real stg_bubble__partner.partner_id1, and
-- prod_chapter_mapping has no duplicate worknode_id for engine = 'E2' (no fan-out risk).
-- The remaining 3894 - 68 = 3826 rows (no school_id and no worknode->E2-chapter match)
-- stay excluded, same policy as the null-required-FK cases in the sibling migration
-- models.
--
-- school_academic_year_id_id: no source column carries this FK directly, but bubble_raw
-- does have its own free-text "academic_year" column (distinct from the academic_year
-- table) - populated '2025-2026' on 1508 rows, blank on the rest (bubble has never
-- written '2026-2027' into it). Nullable on the target (the .md notes this FK was made
-- nullable in migration 0023 for exactly this reason), so this is a best-effort
-- backfill, not a hard requirement - step 1 of a larger effort. Three cases, in order:
--   1. Raw academic_year is set (1508 rows): trust it outright - look up
--      school_academic_year_id for (school_id, this exact label). No date guessing
--      needed; bubble already told us the year.
--   2. Raw academic_year is blank AND this row is_active/not removed AND it's the only
--      active row for its school (14 rows): no year-history to disambiguate against -
--      whichever school_academic_year is currently active (is_active=true, removed=
--      false) for that school is necessarily the one this row belongs to. Falls through
--      to case 3 if the school has no currently-active row (e.g. dropped-out chapters).
--   3. Everything else (241 rows) - date-based fallback via school_progression: bubble's
--      academic_year table carries no date range at all (just a label and a global
--      is_active flag - see prod_academic_year_migration.sql), so there's no direct date
--      lookup; school_progression instead uses each school's own currently active
--      (is_active=true, removed=false) 2026-2027 school_academic_year row's created_at as
--      that school's progression timestamp (confirmed on real data: a school's
--      2025-2026 row is deactivated at the exact same timestamp its 2026-2027 row is
--      created - one atomic operation, e.g. school_id 56 at 2026-04-17 05:54:20 for
--      both). A row resolves against '2026-2027' only if its school has such a row AND
--      its own created_at falls on/after that timestamp; otherwise '2025-2026' -
--      including schools whose 2026-2027 row exists but is inactive/removed (progressed
--      then reverted - e.g. school_id 185-188), treated as not-yet-progressed.
--      An earlier version of this case used one global cutover date for every school
--      instead of each school's own progression timestamp; checking it against actual
--      per-school progression showed it was wrong for 57 rows - 36 of which it had
--      already (incorrectly) resolved to the next year's school_academic_year_id, i.e.
--      real mis-assignments, not just coverage gaps.
--
-- All three cases resolve via the same academic_year_lookup (school_id, label) ->
-- school_academic_year_id, taking the most recently updated id per pair -
-- dim_school_academic_year_status documents why more than one PK can exist for the same
-- (school_id, academic_year_id) business key. Resolves 1749/1763 (99%) of rows as of
-- this build; the remaining 14 are all school_id 362, which has zero school_academic_year
-- rows at all in bubble_raw (not a case-logic gap) - left null here, a candidate for a
-- later recovery step once this baseline is reviewed.
--
-- academic_year (helper column, not a real target field - for review during this
-- backfill effort only, expected to be dropped once school_academic_year_id is trusted):
-- passed through exactly as bubble has it (mostly blank), not overwritten with the
-- guessed label above - keeping it honest to source lets a reviewer see which rows were
-- resolved by trusting bubble (case 1) vs one of the two guesses (cases 2/3) by cross-
-- referencing against school_academic_year_id instead of being told a manufactured label.
--
-- volunteer_id_id: 3 rows have a NULL raw volunteer_id. volunteer_id_id is NOT NULL on
-- the target, so these are excluded too (on top of the school_id exclusions above).
--
-- created_by/updated_by: 8 rows have a null raw Created_By, and ~51 more carry a
-- Created_By UUID with no matching row in bubble_raw."user". Falls back to the 'admin'
-- user (user_id 477022) whenever unresolved, same rule as the other migration models,
-- even though created_by is nullable on this target (unlike most other tables) - kept
-- for consistency across all migration models. bubble tracks no separate "modified by"
-- actor, so updated_by mirrors created_by.
--
-- Raw school_volunteer_id is not unique in bubble_raw (5500 rows / 5496 distinct ids as
-- of this build) - deduplicated to one row per id, keeping the latest by Modified_Date.

with raw as (
    select
        "school_volunteer_id"::bigint as school_volunteer_id,
        "school_id" as school_uuid,
        "volunteer_id" as volunteer_uuid,
        "is_active"::boolean as is_active,
        "removed"::boolean as removed,
        nullif("academic_year", '') as raw_academic_year,
        "Created_By" as created_by_uuid,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'school_volunteer') }}
),

partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
user_map as (
    select user_id as uuid, user_id_number, worknode_id
    from {{ ref('stg_bubble__user') }}
),
worknode_school_map as (
    -- E2-only recovery path for rows with no school_id on the raw record - see the
    -- header comment for why this id space (unlike the bubble-chapter path) lines up
    -- with school_id.
    select worknode_id, chapter_id::bigint as school_id
    from {{ ref('prod_chapter_mapping') }}
    where engine = 'E2'
      and worknode_id is not null
),
academic_year_map as (
    select academic_year_id, label as academic_year
    from {{ ref('prod_academic_year_migration') }}
),
academic_year_lookup as (
    -- one row per (school_id, academic_year label): the most recently updated
    -- school_academic_year_id for that pair - see the header comment for why more than
    -- one PK can exist per (school_id, academic_year_id) business key.
    select distinct on (say.school_id, aym.academic_year)
        say.school_id,
        aym.academic_year,
        say.school_academic_year_id
    from {{ ref('prod_school_academic_year_migration') }} say
    join academic_year_map aym on say.academic_year_id = aym.academic_year_id
    order by say.school_id, aym.academic_year, say.updated_at desc
),
school_progression as (
    -- when a school actually progressed into 2026-2027: the created_at of its currently
    -- active (is_active=true, removed=false) 2026-2027 row - see the header comment.
    -- Schools with no such row (never progressed, or progressed then reverted) simply
    -- aren't in this CTE, so their volunteers all default to 2025-2026 below.
    select say.school_id, say.created_at as progressed_at
    from {{ ref('prod_school_academic_year_migration') }} say
    join academic_year_map aym on say.academic_year_id = aym.academic_year_id
    where aym.academic_year = '2026-2027'
      and say.is_active = true
      and say.removed = false
),
current_active_academic_year as (
    -- one row per school currently on: whichever school_academic_year is is_active=true,
    -- removed=false right now for that school (case 2 in the header comment).
    select school_id, school_academic_year_id
    from {{ ref('prod_school_academic_year_migration') }}
    where is_active = true
      and removed = false
),

joined as (
    select
        raw.school_volunteer_id,
        coalesce(partner_map.school_id, worknode_school_map.school_id) as school_id,
        volunteer_map.user_id_number as volunteer_id,
        raw.is_active,
        raw.removed,
        raw.raw_academic_year,
        raw.created_at,
        raw.updated_at,
        coalesce(created_by_map.user_id_number, 477022) as resolved_user_id
    from raw
    left join partner_map on raw.school_uuid = partner_map.uuid
    left join user_map as volunteer_map on raw.volunteer_uuid = volunteer_map.uuid
    left join worknode_school_map on volunteer_map.worknode_id = worknode_school_map.worknode_id
    left join user_map as created_by_map on raw.created_by_uuid = created_by_map.uuid
    where coalesce(partner_map.school_id, worknode_school_map.school_id) is not null
      and volunteer_map.user_id_number is not null
),

school_active_counts as (
    -- how many active, non-removed school_volunteer rows exist per school - used to spot
    -- the single-active-record case (case 2 in the header comment).
    select school_id, count(*) filter (where is_active and not removed) as active_count
    from joined
    group by school_id
),

with_school_academic_year_id as (
    select
        joined.*,
        case
            when joined.raw_academic_year is not null
                then case1.school_academic_year_id
            when joined.is_active and not joined.removed
                and school_active_counts.active_count = 1
                and current_active_academic_year.school_academic_year_id is not null
                then current_active_academic_year.school_academic_year_id
            else case3.school_academic_year_id
        end as school_academic_year_id
    from joined
    left join school_active_counts on joined.school_id = school_active_counts.school_id
    left join academic_year_lookup as case1
        on joined.school_id = case1.school_id
       and joined.raw_academic_year = case1.academic_year
    left join current_active_academic_year
        on joined.school_id = current_active_academic_year.school_id
    left join school_progression
        on joined.school_id = school_progression.school_id
    left join academic_year_lookup as case3
        on joined.school_id = case3.school_id
       and case3.academic_year = (
               case
                   when school_progression.progressed_at is not null
                       and joined.created_at >= school_progression.progressed_at
                       then '2026-2027'
                   else '2025-2026'
               end
           )
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='with_school_academic_year_id',
        partition_by='school_volunteer_id',
        order_by='updated_at desc',
       )
    }}
)

select
    school_volunteer_id,
    school_id,
    volunteer_id as volunteer_id,
    raw_academic_year as academic_year,
    school_academic_year_id,
    is_active,
    removed,
    cast(null as timestamptz) as deleted_at,
    created_at,
    updated_at,
    resolved_user_id as created_by_id,
    resolved_user_id as updated_by_id
from deduplicated
