{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter per week, sourced from prod_do_weekly_sessions.
-- Counts add up across weeks and within a month; the % columns are per row and never summed.
-- Planned includes cancelled dates; "Sessions not held" = planned - held, of which "Cancelled". "Month" is the month holding the week's Thursday (see the source model), for a month filter.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    month_label                     as "Month",
    month_start                     as "Month starting",
    sessions_planned                as "Sessions planned",
    sessions_held                   as "Sessions held",
    sessions_planned - sessions_held as "Sessions not held",
    sessions_cancelled              as "Cancelled",
    sessions_held_by_substitute     as "Sessions held by a substitute",
    pct_sessions_held               as "% of planned sessions held",
    pct_sessions_cancelled          as "% of planned sessions cancelled"
from {{ ref('prod_do_weekly_sessions') }}
