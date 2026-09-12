{{ config(materialized='table') }}

-- Deduplicates school_volunteer records
-- Flow: stg_bubble__school_volunteer → int_bubble__school_volunteer
-- school_id, volunteer_id, and school_academic_year_id come pre-resolved from sessionops_raw
-- (real bigint FKs) - no more UUID joins needed here now that bronze sources from
-- sessionops_raw.
-- is_active is carried through (not just is_removed) because fct_e2_volunteer_recruitment
-- treats is_active = false + is_removed = false as "archived last year" (2025-2026) volunteers --
-- this entity stopped being the recruitment source of truth after that year.

with joined as (
    select
        raw.school_volunteer_id,
        raw.school_academic_year_id,
        raw.school_id,
        raw.volunteer_id,
        raw.is_active,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_volunteer') }} raw
),

-- D1(c): school_volunteer_id collisions observed in review (37 IDs / 96 rows, 2026-08-22) did not
-- reproduce on 2026-08-26 (0 collisions) -- see SRI_DASHBOARD_SPEC.md D1(c) resolution. Repeat
-- (school_id, volunteer_id, academic_year) triples are state changes over time, not corruption, so
-- the dedup key stays school_volunteer_id rather than chasing a natural key. The original defect
-- was an arbitrary tiebreak on ties in modified_date; this order_by makes the survivor deterministic
-- and meaningful: active wins, then non-removed, then most recent, then a stable id.
deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_volunteer_id',
        order_by='is_active desc, is_removed asc, modified_date desc, school_volunteer_id desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_volunteer_id']) }} as school_volunteer_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    {{ dbt_utils.generate_surrogate_key(['volunteer_id']) }} as volunteer_sk,
    school_volunteer_id,
    school_academic_year_id,
    school_id,
    volunteer_id,
    is_active,
    is_removed,
    created_date,
    modified_date
from deduplicated
