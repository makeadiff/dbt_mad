{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per child per chapter per period, sourced from
-- prod_do_child_sessions. First name and child ID only; no surname. "Period" is a tab-level
-- choice in Dalgo; "Period order" sorts it.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    period_label                    as "Period",
    period_sort                     as "Period order",
    child_first_name                as "First name",
    child_id                        as "Child ID",
    sessions_held                   as "Sessions held in their mentoring circles",
    sessions_attended               as "Sessions attended",
    attendance_pct                  as "% of sessions attended",
    hours_of_support                as "Hours of support",
    status                          as "Status"
from {{ ref('prod_do_child_sessions') }}
