{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- Dalgo cannot rename a column or a pie slice after the fact -- whatever text lands here is what
-- ops sees on the dashboard, so every name below is final vocabulary, not a dbt-convention name.
-- Quoted mixed-case identifiers are deliberate and safe: this is a leaf model (grep -rl
-- "ref('prod_sric_chapters_display')" models/ should always come back empty), so there is no
-- downstream SQL that would have to quote them back.
--
-- Grain: one row per chapter (matches prod_sric_dashboard_data's enforced grain, see
-- assert_sric_dashboard_data_unique_chapter_id).
--
-- Two boolean/text pairs are intentional, not redundant: the "?" columns are booleans for Dalgo's
-- filter widgets; the label columns below are the display text a pie chart renders as its slice
-- legend -- Dalgo shows a boolean's raw true/false if you hand it one directly, so the label text
-- has to be built here, in the model, not left to the chart config.
select
    chapter                                                        as "Chapter",
    city                                                            as "City",
    chapter_organiser                                               as "Chapter organiser",
    chapter_status                                                  as "Chapter running this year?",
    case
        when chapter_status then 'Running this year'
        else 'Not this year'
    end                                                             as "Chapter running this year",
    classes_set_up                                                  as "Chapter has classes ready?",
    case
        when classes_set_up then 'Classes ready'
        else 'Still setting up'
    end                                                             as "Chapter has classes ready",
    active_class_sections                                           as "Class sections",
    total_children                                                  as "Children enrolled",
    children_waiting                                                as "Children waiting for a class",
    volunteers_allocated_to_school                                  as "Volunteers placed at a school",
    volunteers_new_this_year                                        as "Volunteers new this year",
    volunteers_continuing                                           as "Volunteers returning from last year",
    volunteers_assigned_to_class                                    as "Volunteers placed in a class",
    volunteers_unallocated                                          as "Volunteers waiting for a class",
    volunteers_compliant                                            as "Volunteers who signed child safety policies",
    -- Derived action columns (2026-09-12): computed here, not carried as their own columns in
    -- prod_sric_dashboard_data -- both are one-off Dalgo action lists, not general-purpose
    -- analytics figures.
    volunteers_allocated_to_school - volunteers_compliant           as "Volunteers who have not signed child safety policies",
    volunteers_allocated_to_school - volunteers_assigned_to_class   as "Volunteers placed at a school but not yet in a class",
    volunteers_required                                             as "Volunteers needed",
    volunteers_still_to_recruit                                     as "Volunteers still to recruit",
    recruitment_target_met_pct                                      as "Volunteers as % of needed",
    leads_applied_via_link                                          as "People who applied through this chapter's link",
    leads_required                                                  as "Applications needed",
    leads_still_to_source                                           as "Applications still needed"
from {{ ref('prod_sric_dashboard_data') }}
