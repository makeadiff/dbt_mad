{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `users` table.
-- Sourced directly from bubble_raw (not stg_bubble__user / int_bubble__user), same reason as
-- every other model in this chain: bronze casts Created_Date/Modified_Date to ::date,
-- discarding the time-of-day precision the target's TIMESTAMPTZ columns need - see
-- prod_academic_year_migration.sql.
--
-- user_id: this is the model every other migration model in this chain already depends on.
-- Every sibling model's created_by/updated_by/volunteer_id/co_id resolves an unmatched UUID
-- to user_id_number (bubble's own numeric user_id), and falls back to a hardcoded
-- "admin" user_id of 477022 - that id only means anything once a users row actually exists
-- with user_id = 477022. This model is what makes that true: target user_id is bubble's own
-- user_id (not a fresh Django-assigned auto id), preserved 1:1 so every already-migrated FK
-- across the rest of the chain stays valid. Not unique in bubble_raw as-is (5636 rows / 5633
-- distinct ids as of this build) - deduplicated to one row per id, keeping the latest by
-- Modified_Date, same convention as every sibling model.
--
-- email: bubble_raw.user has no email column at all - user_login is the closest equivalent
-- (sampled values are plain email addresses, e.g. "jella@gmail.com"), and the target's own
-- doc note calls user_login "usually the email". Reused directly for the target's required
-- (non-nullable) email field rather than left null.
--
-- reporting_manager_user_id / reporting_manager_user_login: bubble already stores these as
-- plain values in the same id/login space as this migration's own user_id/user_login (2245/
-- 2396 non-null reporting_manager_user_id rows and 2341/2498 non-null
-- reporting_manager_user_login rows resolve to a real row in bubble_raw.user as of this
-- build). The target fields are plain, unconstrained columns (no DB-level FK - loose
-- reference, same design as Partner.co_id), so both are passed straight through as-is,
-- including the unresolved minority, rather than nulled out or re-resolved.
--
-- is_active: bubble_raw has 99/5636 null rows for this column. Coalesced to true (the
-- target's own field default) rather than false, since a genuinely deactivated/blocked user
-- in bubble already carries an explicit false, not a null.
--
-- added_by / last_login_at / synced_at: no bubble equivalent - left null. added_by and
-- synced_at are Hasura-sync-only concepts (see prod_partner_migration.sql for the same
-- pattern on Partner); last_login_at has no historical bubble signal at all.
--
-- deleted_at/deleted_by_id: left null, same as every other migration model in this chain -
-- there's no reliable "when was this actually removed" signal in bubble (see
-- SCHEMA_GAPS_AND_LEARNINGS.md gap #12), so removal state lives only in is_active above.
--
-- No row exclusions needed: user_id, user_login and user_display_name are all 0/5636 null
-- in bubble_raw as of this build, so every row satisfies the target's NOT NULL columns.

with raw as (
    select
        "user_id"::bigint as user_id,
        "user_display_name" as user_display_name,
        "user_login" as user_login,
        "contact"::text as contact,
        coalesce("is_active"::boolean, true) as is_active,
        "user_role" as user_role,
        "reporting_manager_user_login" as reporting_manager_user_login,
        "reporting_manager_role_code" as reporting_manager_role_code,
        "reporting_manager_user_id"::bigint as reporting_manager_user_id,
        "city" as city,
        "state" as state,
        "center" as center,
        "worknode_id"::integer as worknode_id,
        "Created_Date"::timestamptz as user_created_datetime,
        "Modified_Date"::timestamptz as user_updated_datetime
    from {{ source('bubble_raw', 'user') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='raw',
        partition_by='user_id',
        order_by='user_updated_datetime desc',
       )
    }}
)

select
    user_id,
    user_display_name,
    user_login,
    user_login as email,
    contact,
    is_active,
    user_role,
    reporting_manager_user_login,
    reporting_manager_role_code,
    reporting_manager_user_id,
    cast(null as varchar) as added_by,
    city,
    state,
    center,
    user_created_datetime,
    user_updated_datetime,
    cast(null as timestamptz) as last_login_at,
    cast(null as timestamptz) as synced_at,
    worknode_id,
    cast(null as timestamptz) as deleted_at,
    cast(null as bigint) as deleted_by_id
from deduplicated
