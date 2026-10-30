{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter per week, sourced from prod_do_session_breakdown.
-- Counts add up across weeks (the pie sums them); the % columns are per row and never summed.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    sessions_planned                as "Sessions planned",
    sessions_cancelled              as "Cancelled",
    sessions_held_by_original       as "Held by the original volunteer",
    sessions_held_by_substitute     as "Held by a substitute",
    sessions_volunteer_absent       as "Volunteer absent",
    sessions_no_attendance_logged   as "No attendance logged",
    sessions_held                   as "Sessions held",
    pct_held_by_original            as "% of planned sessions held by the original volunteer",
    pct_held_by_substitute          as "% of planned sessions held by a substitute",
    pct_held                        as "% of planned sessions held",
    pct_cancelled                   as "% of planned sessions cancelled"
from {{ ref('prod_do_session_breakdown') }}
