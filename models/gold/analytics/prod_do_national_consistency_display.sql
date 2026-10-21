{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per period, sourced from prod_do_national_consistency, so a
-- national tile follows the Period filter.

select
    period_label                        as "Period",
    period_sort                         as "Period order",
    volunteers_with_planned_sessions    as "Volunteers with sessions planned",
    volunteers_consistent               as "Consistent volunteers",
    pct_volunteers_consistent           as "% of volunteers consistent",
    target_pct_volunteers_consistent    as "Target % of volunteers consistent"
from {{ ref('prod_do_national_consistency') }}
