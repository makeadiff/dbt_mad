{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per volunteer per CCI per week, sourced from
-- prod_cci_volunteer_weekly_attendance (DO spec §3.9). Same columns as
-- prod_do_volunteer_weekly_attendance_display, less "Chapter organiser" (CCIs have none), plus
-- "Planned sessions taken": a few CCI volunteers take sessions that weren't planned for them, so
-- "% of planned classes taken" is SUM "Planned sessions taken" / SUM "Sessions planned". Week /
-- Week starting / Month / Month starting match prod_cci_session_breakdown_display. Group per person
-- on "Volunteer ID": two volunteers can share a name.

select
    chapter_name                    as "CCI",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    month_label                     as "Month",
    month_start                     as "Month starting",
    volunteer_name                  as "Volunteer",
    volunteer_user_id               as "Volunteer ID",
    sessions_planned                as "Sessions planned",
    sessions_taken                  as "Sessions taken",
    planned_taken                   as "Planned sessions taken"
from {{ ref('prod_cci_volunteer_weekly_attendance') }}
