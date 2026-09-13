{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- Dalgo cannot rename a column after the fact -- whatever text lands here is what ops sees on the
-- chart, so every name below is final vocabulary, not a dbt-convention name. Quoted mixed-case
-- identifiers are deliberate and safe: this is a leaf model (grep -rl
-- "ref('prod_sric_funnel_display')" models/ should always come back empty), so there is no
-- downstream SQL that would have to quote them back.
--
-- Grain: one row per (funnel_block, stage, chapter_id, volunteer_source) -- unchanged from
-- prod_sric_funnel; this model only renames columns for the chart, it does not reshape rows.
-- volunteer_source is intentionally not exposed here -- the coverage chart facets by it directly
-- off prod_sric_funnel's raw column (§6.1b: never key a chart facet off a display-only rename).
select
    funnel_block       as "Block",
    stage_order        as "Stage order",
    stage_name         as "Stage",
    chapter            as "Chapter",
    city               as "City",
    chapter_status     as "Chapter running this year?",
    volunteers         as "Volunteers",
    conversion_pct     as "Conversion %"
from {{ ref('prod_sric_funnel') }}
