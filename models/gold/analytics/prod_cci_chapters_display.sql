{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI, sourced from prod_cci_chapter_summary; the not-met
-- reason codes become plain labels so Dalgo slices and filters read as words, and a met CCI reads
-- "Met" rather than blank (the metric model keeps NULL). Column order follows the dashboard's story
-- (spec E1 §3.8): children -> mentoring circles -> volunteers -> sessions. No baseline (spec §3.7).

select
    chapter_name                                    as "Chapter",
    city                                            as "City",
    group_name                                      as "Group",
    -- Children
    active_children                                 as "Active children",
    children_foundations                            as "Foundations children",
    children_education_support                      as "Education Support children",
    children_transition_readiness                   as "Transition Readiness children",
    children_programme_not_set                      as "Children with programme not set",
    children_with_volunteer                         as "Children with a volunteer",
    pct_children_with_volunteer                     as "% of children with a volunteer",
    children_in_circle_no_volunteer                 as "Children in a mentoring circle with no volunteer",
    children_not_in_circle                          as "Children not yet in a mentoring circle",
    -- Mentoring circles
    sections_no_circle                              as "Sections with no mentoring circle",
    mentoring_circles                               as "Mentoring circles",
    circles_no_volunteer                            as "Mentoring circles with no volunteer",
    -- Volunteers
    volunteers_needed_min_1to5                      as "Volunteers needed (minimum, 1:5)",
    volunteers_needed_as_set_up_1to5                as "Volunteers needed (as set up, 1:5)",
    volunteers_needed_as_set_up_2to5                as "Volunteers needed (as set up, 2:5)",
    volunteers_placed                               as "Volunteers placed in a mentoring circle",
    case when volunteer_requirement_met_1to5 then 'Met' else 'Not met' end
                                                    as "Volunteer requirement (1:5)",
    case when volunteer_requirement_met_2to5 then 'Met' else 'Not met' end
                                                    as "Volunteer requirement (2:5)",
    coalesce(volunteer_requirement_not_met_reason, 'Met')
                                                    as "Why volunteer requirement not met",
    -- Sessions
    circles_started                                 as "Mentoring circles started",
    pct_circles_started                             as "% of mentoring circles started",
    case when has_sessions_started then 'Yes' else 'No' end
                                                    as "Sessions started",
    case when all_circles_started then 'Yes' else 'No' end
                                                    as "Every mentoring circle started",
    coalesce(not_all_circles_started_reason, 'Every circle started')
                                                    as "Why not every mentoring circle started",
    sessions_held_since_1_oct                       as "Sessions held since 1 October"
from {{ ref('prod_cci_chapter_summary') }}
