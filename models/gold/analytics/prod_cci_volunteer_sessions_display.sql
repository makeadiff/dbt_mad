{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per volunteer per CCI per period, sourced from
-- prod_cci_volunteer_sessions (spec E1 §3.10). No planned or consistency columns until the slot
-- link lands. "Period" is a tab-level choice in Dalgo; "Period order" sorts it.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    period_label                    as "Period",
    period_sort                     as "Period order",
    volunteer_name                  as "Volunteer",
    sessions_taken                  as "Sessions taken",
    hours_contributed               as "Hours contributed"
from {{ ref('prod_cci_volunteer_sessions') }}
