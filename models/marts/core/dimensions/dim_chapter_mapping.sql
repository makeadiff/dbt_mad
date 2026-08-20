{{ config(materialized='table') }}

-- dim_chapter_mapping: one row per chapter, combining Bubble partner data with Google Sheet ops-mapping
-- Grain: one row per chapter_id
-- Ported from the mapping_sheet_deduped + chapter_dimensions CTEs in the legacy fct_dashboard_summary
-- model. Base is int_bubble__partner (every bubble school/partner), enriched with mapping-sheet
-- fields for everything Bubble doesn't have (city/engine/status) and worknode_id (the sheet's real
-- role: chapter_id <-> worknode_id mapping for the PC user sync, nothing more).
-- chapter_name is sourced from Bubble (partner_name), not the mapping sheet: the sheet is a
-- human-maintained spreadsheet and has real copy-paste-grade name mismatches against Bubble for a
-- meaningful subset of chapters (confirmed by checking the actual data), where Bubble is the
-- system of record. Falls back to the sheet's chapter_name only in the rare case partner_name
-- itself is null.
-- state is the reverse: sheet's state is only filled for 80 of 562 chapters vs. Bubble's 562/562
-- (confirmed by checking the actual data), so it falls back to Bubble's state whenever the sheet's
-- is null.
-- co_name/cho_name no longer come from the sheet (sheet only ever carried co_id/cho_id, resolved to
-- a name via a PC user join anyway -- the sheet added no information here, just another hop with a
-- chance to be stale for the 454/561 partners it has no row for at all).
--   - co_name: partner.co_id already ties a chapter to its CO directly, and Bubble's own partner
--     record carries the resolved co_name as a plain text field -- no join required.
--   - cho_name: no such direct field exists on partner, so it's resolved the same way the CHO role
--     is resolved everywhere else -- the active Bubble user with user_role = 'CHO' whose worknode_id
--     matches this chapter's worknode_id (worknode_id itself still comes from the sheet, since that
--     chapter_id <-> worknode_id mapping is the one thing the sheet is actually the source for).

with mapping_sheet_deduped as (
    -- one row per chapter_id: prefer the currently-valid mapping row when duplicates exist
    select distinct on (chapter_id)
        chapter_id,
        chapter_name,
        city_name,
        state,
        engine,
        chapter_status,
        worknode_id
    from {{ ref('int_google_sheet__chapter_mapping') }}
    where chapter_id is not null
    order by chapter_id, validation_status desc
),

cho_lookup as (
    -- one row per worknode_id: the current active CHO for that worknode
    select distinct on (worknode_id)
        worknode_id,
        user_display_name as cho_name
    from {{ ref('int_bubble__user') }}
    where user_role = 'CHO'
        and is_active = true
        and worknode_id is not null
    order by worknode_id, modified_date desc
)

select
    p.partner_id::text as chapter_id,
    coalesce(p.partner_name, mm.chapter_name) as chapter_name,
    mm.city_name,
    coalesce(mm.state, p.state) as state,
    p.co_name,
    cho.cho_name,
    mm.engine,
    mm.chapter_status,
    mm.worknode_id
from {{ ref('int_bubble__partner') }} p
left join mapping_sheet_deduped mm
    on p.partner_id::text = mm.chapter_id
left join cho_lookup cho
    on mm.worknode_id = cho.worknode_id
