{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per district plus an "All districts" row, sourced from
-- prod_cci_district_summary (spec E1 §3.8, §7 D3). Each % follows its count and is a ratio of sums.
-- "District order" puts All districts last: the Dalgo table sorts on it and hides it.

select
    district                                        as "District",
    do_name                                         as "DO",
    ccis                                            as "CCIs",
    active_children                                 as "Active children",
    children_foundations                            as "Foundations children",
    children_education_support                      as "Education Support children",
    children_transition_readiness                   as "Transition Readiness children",
    children_programme_not_set                      as "Children with programme not set",
    children_with_volunteer                         as "Children with a volunteer",
    pct_children_with_volunteer                     as "% of children with a volunteer",
    children_in_circle_no_volunteer                 as "Children in a mentoring circle with no volunteer",
    children_not_in_circle                          as "Children not yet in a mentoring circle",
    sections_no_circle                              as "Sections with no mentoring circle",
    mentoring_circles                               as "Mentoring circles",
    circles_no_volunteer                            as "Mentoring circles with no volunteer",
    circles_needing_volunteer                       as "Mentoring circles still needing a volunteer",
    circles_one_volunteer                           as "Mentoring circles with 1 volunteer",
    circles_two_plus_volunteers                     as "Mentoring circles with 2 or more volunteers",
    volunteers_needed_min_1to5                      as "Volunteers needed (minimum, 1:5)",
    volunteers_needed_as_set_up_1to5                as "Volunteers needed (as set up, 1:5)",
    volunteers_placed                               as "Volunteers placed in a mentoring circle",
    ccis_requirement_met_1to5                       as "CCIs meeting the volunteer requirement",
    pct_ccis_requirement_met_1to5                   as "% of CCIs meeting the volunteer requirement",
    circles_started                                 as "Mentoring circles started",
    pct_circles_started                             as "% of mentoring circles started",
    ccis_sessions_started                           as "CCIs with sessions started",
    pct_ccis_sessions_started                       as "% of CCIs with sessions started",
    ccis_all_circles_started                        as "CCIs with every mentoring circle started",
    pct_ccis_all_circles_started                    as "% of CCIs with every mentoring circle started",
    circles_not_started                             as "Mentoring circles not started",
    circles_stopped_logging                         as "Mentoring circles with sessions before 1 October, none since",
    sessions_held_since_1_oct                       as "Sessions held since 1 October",
    district_sort                                   as "District order"
from {{ ref('prod_cci_district_summary') }}
