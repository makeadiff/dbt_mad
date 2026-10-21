{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- One-row national tiles for the DO dashboard, sourced from prod_do_national_kpis. No chapter
-- column by design (same reason as prod_sric_national_display): a Chapter filter then simply
-- doesn't apply to these tiles.

select
    chapters_total                  as "Chapters",
    chapters_met_1to5               as "Chapters meeting volunteer requirement (1:5)",
    pct_chapters_met_1to5           as "% of chapters meeting volunteer requirement (1:5)",
    chapters_met_2to5               as "Chapters meeting volunteer requirement (2:5)",
    pct_chapters_met_2to5           as "% of chapters meeting volunteer requirement (2:5)",
    target_pct_chapters_met         as "Target % of chapters",
    chapters_sessions_commenced     as "Chapters with sessions commenced",
    pct_chapters_sessions_commenced as "% of chapters with sessions commenced",
    target_pct_sessions_commenced   as "Target % of chapters with sessions commenced",
    active_children                 as "Active children",
    children_covered                as "Children covered",
    pct_children_covered            as "% of children covered",
    target_pct_children_covered     as "Target % of children covered"
from {{ ref('prod_do_national_kpis') }}
