{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter, sourced from prod_do_chapter_summary; the
-- not-met reason codes become plain labels here so Dalgo slices and filters read as words, and a
-- met chapter reads "Met" rather than blank (the metric model keeps NULL). The two "no volunteer"
-- columns stay here but are left out of the Dalgo table (always 0 today).
-- Shows the CHO only, as "Chapter organiser"; co_name stays in the metric model for the CO rollup.

select
    chapter_name                                                        as "Chapter",
    city                                                                as "City",
    cho_name                                                            as "Chapter organiser",
    district                                                            as "District",
    do_name                                                             as "DO",
    mentoring_circles_total                                             as "Mentoring circles",
    mentoring_circles_with_slot                                         as "Mentoring circles with a slot",
    mentoring_circles_no_slot                                           as "Mentoring circles with no slot",
    mentoring_circles_0_volunteers                                      as "Mentoring circles with no volunteer",
    mentoring_circles_1_volunteer                                       as "Mentoring circles with 1 volunteer",
    mentoring_circles_2plus_volunteers                                  as "Mentoring circles with 2 or more volunteers",
    case when volunteer_requirement_met_1to5 then 'Yes' else 'No' end   as "Volunteer requirement met (1:5)",
    case when volunteer_requirement_met_2to5 then 'Yes' else 'No' end   as "Volunteer requirement met (2:5)",
    case volunteer_requirement_not_met_reason
        when 'no_data' then 'No data this year'
        when 'no_slots' then 'No slots set up'
        when 'no_circles' then 'No mentoring circles set up'
        when 'circles_with_no_slot' then 'Some mentoring circles have no slot'
        when 'circles_without_volunteer' then 'Some circles have no volunteer'
        else 'Met'
    end                                                                 as "Why volunteer requirement not met",
    mentoring_circles_not_yet_due                                       as "Mentoring circles with first session upcoming",
    mentoring_circles_not_started                                       as "Mentoring circles not started",
    mentoring_circles_no_session_since_1_oct                            as "Mentoring circles with no session since 1 October",
    pct_mentoring_circles_started                                       as "% of mentoring circles started",
    case when sessions_commenced_met then 'Yes' else 'No' end           as "Sessions commenced",
    case sessions_commenced_not_met_reason
        when 'no_data' then 'No data this year'
        when 'no_slots' then 'No slots set up'
        when 'no_circles' then 'No mentoring circles set up'
        when 'circles_not_started' then 'Some mentoring circles haven''t started'
        when 'circles_not_yet_due' then 'First session upcoming'
        else 'Met'
    end                                                                 as "Why sessions not commenced",
    active_children                                                     as "Active children",
    children_covered                                                    as "Children covered",
    pct_children_covered                                                as "% of children covered",
    children_in_circle_no_volunteer                                     as "Children in a mentoring circle with no volunteer",
    children_in_circle_no_slot                                          as "Children in a mentoring circle with no slot",
    children_in_no_class_section                                        as "Children in no class section"
from {{ ref('prod_do_chapter_summary') }}
