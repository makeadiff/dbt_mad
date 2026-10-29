{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per district plus "All districts", sourced from
-- prod_do_district_summary. Each % follows its count. "District order" puts All districts last:
-- the Dalgo table sorts on it and hides it.

select
    district                        as "District",
    do_name                         as "DO",
    chapters                        as "Chapters",
    active_children                 as "Children",
    children_covered                as "Children with a volunteer",
    pct_children_covered            as "% of children with a volunteer",
    chapters_no_slots               as "Chapters with no slots",
    volunteers_in_chapter           as "Volunteers signed up",
    volunteers_allocated            as "Volunteers placed",
    pct_volunteers_placed           as "% of volunteers placed",
    chapters_met_1to5               as "Chapters where every mentoring circle has a volunteer",
    pct_chapters_met_1to5           as "% of chapters where every mentoring circle has a volunteer",
    mentoring_circles_started       as "Mentoring circles started",
    pct_mentoring_circles_started   as "% of mentoring circles started",
    sessions_planned                as "Sessions planned since 1 October",
    sessions_held                   as "Sessions held since 1 October",
    sessions_no_attendance          as "Sessions with no attendance logged since 1 October",
    pct_sessions_held               as "% of planned sessions held since 1 October",
    district_sort                   as "District order"
from {{ ref('prod_do_district_summary') }}
