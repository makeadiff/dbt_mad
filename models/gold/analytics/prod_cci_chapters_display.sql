{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI, sourced from prod_cci_chapter_summary; the not-met
-- reason codes become plain labels so Dalgo slices and filters read as words, and a met CCI reads
-- "Met" rather than blank (the metric model keeps NULL). Column order follows the dashboard's story
-- (spec E1 §3.8): children -> mentoring circles and slots -> volunteers -> sessions. No baseline (spec §3.7).
-- Layout v4 (spec §7 L1-L7, School's names from DO_DASHBOARD_SPEC.md §3.8): setup complete, weekly
-- slots, children in a circle with no weekly slot, minimum volunteers, leads and the planned-based
-- session columns are new; volunteer names follow School's ("Minimum volunteers to start sessions",
-- "Volunteers needed, one per mentoring circle" = CCI's as-set-up figure per L2, "Volunteers
-- assigned to a class section"); the 2:5 columns are dropped. No "Volunteers mapped to a chapter"
-- column (L3: no year-scoped source in Platform Commons). "Leads in this city" is a city total
-- repeated on every CCI in the city: a table column, never summed in a tile.

select
    chapter_name                                    as "Chapter",
    city                                            as "City",
    district                                        as "District",
    do_name                                         as "DO",
    -- Children
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
    case when setup_complete then 'Yes' else 'No' end
                                                    as "Setup complete",
    -- Mentoring circles and slots
    weekly_session_slots                            as "Weekly session slots",
    sections_no_circle                              as "Sections with no mentoring circle",
    mentoring_circles                               as "Mentoring circles",
    circles_with_weekly_slot                        as "Mentoring circles with a weekly slot",
    circles_no_weekly_slot                          as "Mentoring circles with no weekly slot",
    circles_no_volunteer                            as "Mentoring circles with no volunteer",
    circles_needing_volunteer                       as "Mentoring circles still needing a volunteer",
    circles_one_volunteer                           as "Mentoring circles with 1 volunteer",
    circles_two_plus_volunteers                     as "Mentoring circles with 2 or more volunteers",
    -- Volunteers
    volunteers_minimum_to_start                     as "Minimum volunteers to start sessions",
    volunteers_needed_as_set_up_1to5                as "Volunteers needed, one per mentoring circle",
    volunteers_placed                               as "Volunteers assigned to a class section",
    leads_chapter_link                              as "Leads from this chapter's link",
    leads_city                                      as "Leads in this city",
    case when volunteer_requirement_met_1to5 then 'Met' else 'Not met' end
                                                    as "Volunteer requirement (1:5)",
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
    circles_not_started                             as "Mentoring circles not started",
    circles_stopped_logging                         as "Mentoring circles with sessions before 1 October, none since",
    sessions_planned_since_1_oct                    as "Sessions planned since 1 October",
    sessions_held_since_1_oct                       as "Sessions held since 1 October",
    sessions_planned_since_1_oct - sessions_held_since_1_oct
                                                    as "Sessions not held since 1 October",
    sessions_cancelled_since_1_oct                  as "Sessions cancelled since 1 October",
    pct_sessions_held_since_1_oct                   as "% of planned sessions held since 1 October"
from {{ ref('prod_cci_chapter_summary') }}
