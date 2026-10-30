{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI per week, sourced from prod_cci_session_breakdown
-- (spec E1 §3.10). Counts add up across weeks; the % columns are per row and never summed. No
-- planned-based columns until the slot link lands.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    sessions_held                   as "Sessions held",
    sessions_held_by_original       as "Held by the original volunteer",
    sessions_held_by_substitute     as "Held by a substitute",
    sessions_cancelled              as "Cancelled",
    pct_held_by_original            as "% of sessions held by the original volunteer",
    pct_held_by_substitute          as "% of sessions held by a substitute"
from {{ ref('prod_cci_session_breakdown') }}
