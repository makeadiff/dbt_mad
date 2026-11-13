{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per child per chapter per week, sourced from
-- prod_do_child_weekly_attendance (spec §3.9). First name and child ID only; no surname. Week /
-- Week starting / Month / Month starting match prod_do_session_breakdown_display, so the dashboard
-- Week and Month filters apply. Counts add up across weeks; % and Status are computed in Dalgo over
-- the weeks picked.

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
    child_first_name                as "First name",
    child_id                        as "Child ID",
    sessions_held                   as "Sessions held in their mentoring circles",
    sessions_attended               as "Sessions attended"
from {{ ref('prod_do_child_weekly_attendance') }}
