{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per volunteer per chapter per period, sourced from
-- prod_do_volunteer_sessions. "Period" is a tab-level choice in Dalgo; "Period order" sorts it.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
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
from {{ ref('prod_do_volunteer_sessions') }}
