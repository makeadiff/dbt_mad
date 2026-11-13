{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per volunteer per chapter per week, sourced from
-- prod_do_volunteer_weekly_attendance (spec §3.9). Week / Week starting / Month / Month starting
-- match prod_do_session_breakdown_display, so the dashboard Week and Month filters apply. Counts add
-- up across weeks; % and Status are computed in Dalgo over the weeks picked, grouped on "Volunteer
-- ID" (two volunteers in a chapter can share a name).

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
    volunteer_name                  as "Volunteer",
    volunteer_id                    as "Volunteer ID",
    sessions_planned                as "Sessions planned",
    sessions_taken                  as "Sessions taken"
from {{ ref('prod_do_volunteer_weekly_attendance') }}
