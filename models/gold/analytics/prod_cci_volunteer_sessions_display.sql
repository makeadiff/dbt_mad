{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per volunteer per CCI per period, sourced from
-- prod_cci_volunteer_sessions (spec E1 §3.10, §8 2026-11-12 (later) ruling 2). Same columns and order
-- as prod_do_volunteer_sessions_display, less "Chapter organiser" (CCIs have none), so School charts
-- 3551-3557 can be cloned onto 639. Planned is slot-day based until Platform Commons' generated
-- sessions reach the warehouse. "Period" is a tab-level choice in Dalgo; "Period order" sorts it.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    period_label                    as "Period",
    period_sort                     as "Period order",
    volunteer_name                  as "Volunteer",
    sessions_planned                as "Sessions planned",
    sessions_taken                  as "Sessions taken",
    attendance_pct                  as "% of planned sessions taken",
    hours_contributed               as "Hours contributed",
    status                          as "Status"
from {{ ref('prod_cci_volunteer_sessions') }}
