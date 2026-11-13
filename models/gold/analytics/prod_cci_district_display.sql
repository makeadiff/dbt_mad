{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per district plus an "All districts" row, sourced from
-- prod_cci_district_summary (spec E1 §3.8, §7 D3). Each % follows its count and is a ratio of sums.
-- "District order" puts All districts last: the Dalgo table sorts on it and hides it.
-- Layout v4 (spec §7 L1-L7): setup complete, weekly slots, minimum volunteers, leads from CCI links
-- and planned-based sessions are new; volunteer columns take School's names; 2:5 dropped.

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
    children_in_circle_no_slot                      as "Children in a mentoring circle with no weekly slot",
    children_not_in_circle                          as "Children not yet in a mentoring circle",
    ccis_setup_complete                             as "CCIs with setup complete",
    pct_ccis_setup_complete                         as "% of CCIs with setup complete",
    weekly_session_slots                            as "Weekly session slots",
    sections_no_circle                              as "Sections with no mentoring circle",
    mentoring_circles                               as "Mentoring circles",
    circles_with_weekly_slot                        as "Mentoring circles with a weekly slot",
    circles_no_weekly_slot                          as "Mentoring circles with no weekly slot",
    circles_no_volunteer                            as "Mentoring circles with no volunteer",
    circles_needing_volunteer                       as "Mentoring circles still needing a volunteer",
    circles_one_volunteer                           as "Mentoring circles with 1 volunteer",
    circles_two_plus_volunteers                     as "Mentoring circles with 2 or more volunteers",
    volunteers_minimum_to_start                     as "Minimum volunteers to start sessions",
    volunteers_needed_as_set_up_1to5                as "Volunteers needed, one per mentoring circle",
    volunteers_placed                               as "Volunteers assigned to a class section",
    leads_chapter_link                              as "Leads from chapter links",
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
    sessions_planned_since_1_oct                    as "Sessions planned since 1 October",
    sessions_held_since_1_oct                       as "Sessions held since 1 October",
    sessions_planned_since_1_oct - sessions_held_since_1_oct
                                                    as "Sessions not held since 1 October",
    sessions_cancelled_since_1_oct                  as "Sessions cancelled since 1 October",
    pct_sessions_held_since_1_oct                   as "% of planned sessions held since 1 October",
    pct_sessions_cancelled_since_1_oct              as "% of planned sessions cancelled since 1 October",
    -- Baseline (spec §7 B5)
    children_grades_5_to_8                      as "Active children in grades 5 to 8",
    children_with_baseline                          as "Children with a baseline",
    pct_children_with_baseline                      as "% of children with a baseline",
    children_cleared_aser                           as "Children who cleared ASER",
    baseline_forms_submitted                        as "Baseline forms submitted",
    district_sort                                   as "District order"
from {{ ref('prod_cci_district_summary') }}
