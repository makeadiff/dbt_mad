{{ config(materialized='table') }}

-- Migration feed for session-ops platform's `partner` table.
-- Sourced directly from bubble_raw (not stg_bubble__partner / int_bubble__partner) for the
-- same reason as every other model in this migration chain: bronze casts
-- Created_Date/Modified_Date to ::date, discarding the time-of-day precision the target's
-- TIMESTAMPTZ created_at/updated_at need - see prod_academic_year_migration.sql.
--
-- Target schema note: unlike every other model in this chain, session-ops's Partner model
-- (sessionops/models/partner.py) has no created_by/updated_by/removed fields at all - it
-- inherits SoftDeleteBaseModel's is_active/created_at/updated_at/deleted_at/deleted_by only,
-- plus its own Hasura-sync-only fields (partner_created_date, partner_updated_date,
-- synced_at, crm_partner_removed) that have no bubble equivalent - Partner is synced live
-- from Hasura CRM going forward; this model is a one-time historical backfill that predates
-- that sync going live. So there is no "admin fallback" created_by pattern here.
--
-- partner_id: bubble's own numeric partner_id (aliased partner_id1 in stg_bubble__partner) -
-- not bubble's UUID _id. Intentional: partner_id1 is the exact id space every other
-- migration model already treats as "school_id" (see prod_child_migration.sql's
-- partner_map), so target Partner.partner_id must equal it to keep every already-migrated
-- school_id FK valid. 561/561 rows have a unique, non-null partner_id as of this build - no
-- dedup needed.
--
-- co_id: bubble's co_id is a UUID pointing at user._id (a Bubble user - the Chapter Owner),
-- not a Hasura CO id - resolved here via user_map to the same user_id_number space every
-- other migration model's created_by/volunteer_id already resolves to (560/561 non-null
-- co_id rows resolve as of this build). The target field is a plain nullable
-- BigIntegerField with no DB-level FK, so an unresolved co_id is left null rather than
-- falling back to the admin user - admin is not this partner's CO.
--
-- is_active: derived as NOT partner_removed. Partner has no separate "removed" boolean on
-- the target (unlike most other entities in this chain) - is_active is its only lifecycle
-- signal, so bubble's removal flag maps directly onto it (inverted), not left as a parallel
-- field.
--
-- crm_partner_removed / partner_created_date / partner_updated_date / synced_at: all
-- Hasura-sync-only concepts with no bubble equivalent. crm_partner_removed defaults false
-- (matching the target's own default); the three timestamps are left null since nothing has
-- synced from Hasura yet as of this historical backfill.
--
-- deleted_at/deleted_by_id: left null, same as every other migration model in this chain -
-- there's no reliable "when was this actually removed" signal in bubble (see
-- SCHEMA_GAPS_AND_LEARNINGS.md gap #12), so removal state lives only in is_active above.
--
-- No row exclusions needed: partner_name, converted and partner_removed are all 0/561 null
-- in bubble_raw as of this build, so every row satisfies the target's NOT NULL columns.

with raw as (
    select
        "partner_id"::bigint as partner_id,
        "partner_name" as partner_name,
        "co_id" as co_id_uuid,
        "co_name" as co_name,
        "address_line_1" as address_line_1,
        "address_line_2" as address_line_2,
        "city" as city,
        "city_id"::integer as city_id,
        "state" as state,
        "state_id"::integer as state_id,
        "pincode"::integer as pincode,
        "school_type" as school_type,
        "partner_affiliation_type" as partner_affiliation_type,
        "poc_name" as poc_name,
        "poc_email" as poc_email,
        "poc_designation" as poc_designation,
        "poc_contact"::text as poc_contact,
        "mou_sign_date"::date as mou_sign_date,
        "mou_start_date"::date as mou_start_date,
        "mou_end_date"::date as mou_end_date,
        "mou_url" as mou_url,
        "converted"::boolean as converted,
        "latest_conversion_stage" as latest_conversion_stage,
        "lead_source" as lead_source,
        "date_of_first_contact"::timestamptz as date_of_first_contact,
        "confirmed_child_count"::integer as confirmed_child_count,
        "total_child_count"::integer as total_child_count,
        "classes"::text as classes,
        "partner_removed"::boolean as partner_removed,
        "Created_Date"::timestamptz as created_at,
        "Modified_Date"::timestamptz as updated_at
    from {{ source('bubble_raw', 'partner') }}
),

user_map as (
    select user_id as uuid, user_id_number
    from {{ ref('stg_bubble__user') }}
)

select
    raw.partner_id,
    raw.partner_name,
    user_map.user_id_number as co_id,
    raw.co_name,
    raw.address_line_1,
    raw.address_line_2,
    raw.city,
    raw.city_id,
    raw.state,
    raw.state_id,
    raw.pincode,
    raw.school_type,
    raw.partner_affiliation_type,
    raw.poc_name,
    raw.poc_email,
    raw.poc_designation,
    raw.poc_contact,
    raw.mou_sign_date,
    raw.mou_start_date,
    raw.mou_end_date,
    raw.mou_url,
    raw.converted,
    false as crm_partner_removed,
    raw.latest_conversion_stage,
    raw.lead_source,
    raw.date_of_first_contact,
    raw.confirmed_child_count,
    raw.total_child_count,
    raw.classes,
    cast(null as timestamptz) as partner_created_date,
    cast(null as timestamptz) as partner_updated_date,
    cast(null as timestamptz) as synced_at,
    not raw.partner_removed as is_active,
    raw.created_at,
    raw.updated_at,
    cast(null as timestamptz) as deleted_at,
    cast(null as bigint) as deleted_by_id
from raw
left join user_map on raw.co_id_uuid = user_map.uuid
