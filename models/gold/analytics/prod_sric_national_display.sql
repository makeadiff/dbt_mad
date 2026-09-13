{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- One-row national summary (2026-09-13) for Dalgo tiles that need a ratio of sums, not a sum of
-- ratios -- e.g. "Volunteers as % of needed" nationally is
-- SUM(placed) / SUM(needed), not AVG(per-chapter %), which would weight a 2-volunteer chapter the
-- same as a 200-volunteer one. prod_sric_chapters_display already carries the per-chapter %, so
-- this model exists only for the national figure, not to duplicate that one.
--
-- No chapter column by design: this is a single row, so a Chapter filter on the dashboard would
-- either do nothing or (worse) silently zero the whole tile depending on how Dalgo handles a
-- filter with no matching dimension. Leaving the column out entirely means the filter simply
-- doesn't apply to this tile, which is the intended behavior.
--
-- Scope: chapters with classes_set_up AND chapter_status true -- same "genuinely active this
-- year" population prod_sric_national_display's sibling tiles use elsewhere on the dashboard,
-- sourced from prod_sric_dashboard_data so this model doesn't re-derive that filter.
--
-- Grain: exactly one row, always -- there is no group-by key.
with scoped as (
    select *
    from {{ ref('prod_sric_dashboard_data') }}
    where classes_set_up = true
      and chapter_status = true
),

totals as (
    select
        sum(volunteers_allocated_to_school) as volunteers_placed,
        sum(volunteers_required) as volunteers_needed,
        sum(leads_applied_via_link) as leads_applied,
        sum(leads_required) as leads_needed
    from scoped
)

select
    volunteers_placed                                                        as "Volunteers placed at a school",
    volunteers_needed                                                        as "Volunteers needed",
    round(100.0 * volunteers_placed / nullif(volunteers_needed, 0), 1)       as "Volunteers as % of needed",
    leads_applied                                                            as "People who applied through chapter links",
    leads_needed                                                             as "Applications needed",
    round(100.0 * leads_applied / nullif(leads_needed, 0), 1)                as "Applications as % of needed"
from totals
