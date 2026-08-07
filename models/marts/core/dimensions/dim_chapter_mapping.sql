{{ config(materialized='table') }}

-- dim_chapter_mapping: one row per chapter, combining Bubble partner data with Google Sheet ops-mapping
-- Grain: one row per chapter_id
-- Ported from the mapping_sheet_deduped + chapter_dimensions CTEs in the legacy fct_dashboard_summary
-- model. Base is int_bubble__partner (every bubble school/partner), enriched with mapping-sheet
-- fields for everything Bubble doesn't have (city/CO/CHO/engine/status).
-- chapter_name is sourced from Bubble (partner_name), not the mapping sheet: the sheet is a
-- human-maintained spreadsheet and has real copy-paste-grade name mismatches against Bubble for a
-- meaningful subset of chapters (confirmed by checking the actual data), where Bubble is the
-- system of record. Falls back to the sheet's chapter_name only in the rare case partner_name
-- itself is null.
-- state is the reverse: sheet's state is only filled for 80 of 562 chapters vs. Bubble's 562/562
-- (confirmed by checking the actual data), so it falls back to Bubble's state whenever the sheet's
-- is null, rather than sourcing from the sheet alone like city_name/co_name/cho_name/engine/status
-- (those represent the org's ops-reporting grouping, not a raw address field Bubble already has).

with mapping_sheet_deduped as (
    -- one row per chapter_id: prefer the currently-valid mapping row when duplicates exist
    select distinct on (chapter_id)
        chapter_id,
        chapter_name,
        city_name,
        state,
        co_name,
        cho_name,
        engine,
        chapter_status,
        worknode_id
    from {{ ref('int_google_sheet__chapter_mapping') }}
    where chapter_id is not null
    order by chapter_id, validation_status desc
)

select
    p.partner_id::text as chapter_id,
    coalesce(p.partner_name, mm.chapter_name) as chapter_name,
    mm.city_name,
    coalesce(mm.state, p.state) as state,
    mm.co_name,
    mm.cho_name,
    mm.engine,
    mm.chapter_status,
    mm.worknode_id
from {{ ref('int_bubble__partner') }} p
left join mapping_sheet_deduped mm
    on p.partner_id::text = mm.chapter_id
