{{ config(materialized='table') }}

-- CCI dashboard (E1) group rollup, snake_case: one row per group (spec §7 ruling 3: the seed's owner,
-- else city) plus an "All" row that is the sum of the chapters. Feeds the Overview table and goal
-- tiles (spec §3.8). Counts are summed from prod_cci_chapter_summary; every % is a ratio of sums,
-- never an average of chapter %s.
--
-- "CCIs" counts only CCIs with children this year (27, not 28), so the CCI percentages use the same
-- denominator as the spec's national figures. A group whose only CCI has no children (Guntur) shows
-- 0 CCIs and 0 children, so the groups still add up to the All row.
--
-- Grain: one row per group_name, plus 'All'.

with grouped as (
    select
        group_name,
        count(*) filter (where has_children) as ccis,
        sum(active_children) as active_children,
        sum(children_with_volunteer) as children_with_volunteer,
        sum(children_not_in_circle) as children_not_in_circle,
        sum(children_in_circle_no_volunteer) as children_in_circle_no_volunteer,
        sum(children_foundations) as children_foundations,
        sum(children_education_support) as children_education_support,
        sum(children_transition_readiness) as children_transition_readiness,
        sum(children_programme_not_set) as children_programme_not_set,
        sum(sections_no_circle) as sections_no_circle,
        sum(volunteers_needed_min_1to5) as volunteers_needed_min_1to5,
        sum(volunteers_needed_min_2to5) as volunteers_needed_min_2to5,
        sum(volunteers_needed_as_set_up_1to5) as volunteers_needed_as_set_up_1to5,
        sum(volunteers_needed_as_set_up_2to5) as volunteers_needed_as_set_up_2to5,
        sum(volunteers_placed) as volunteers_placed,
        sum(mentoring_circles) as mentoring_circles,
        sum(circles_no_volunteer) as circles_no_volunteer,
        sum(circles_with_weekly_slot) as circles_with_weekly_slot,
        count(*) filter (where volunteer_requirement_met_1to5) as ccis_requirement_met_1to5,
        count(*) filter (where volunteer_requirement_met_2to5) as ccis_requirement_met_2to5,
        sum(circles_started) as circles_started,
        count(*) filter (where has_sessions_started) as ccis_sessions_started,
        count(*) filter (where all_circles_started) as ccis_all_circles_started,
        sum(sessions_held_since_1_oct) as sessions_held_since_1_oct
    from {{ ref('prod_cci_chapter_summary') }}
    group by 1
),

with_all as (
    select 1 as sort_order, g.* from grouped g
    union all
    select
        2 as sort_order,
        'All' as group_name,
        sum(ccis), sum(active_children), sum(children_with_volunteer), sum(children_not_in_circle),
        sum(children_in_circle_no_volunteer), sum(children_foundations), sum(children_education_support),
        sum(children_transition_readiness), sum(children_programme_not_set), sum(sections_no_circle),
        sum(volunteers_needed_min_1to5), sum(volunteers_needed_min_2to5),
        sum(volunteers_needed_as_set_up_1to5), sum(volunteers_needed_as_set_up_2to5),
        sum(volunteers_placed), sum(mentoring_circles), sum(circles_no_volunteer),
        sum(circles_with_weekly_slot), sum(ccis_requirement_met_1to5), sum(ccis_requirement_met_2to5),
        sum(circles_started), sum(ccis_sessions_started), sum(ccis_all_circles_started),
        sum(sessions_held_since_1_oct)
    from grouped
)

select
    w.*,
    round(100.0 * children_with_volunteer / nullif(active_children, 0), 1) as pct_children_with_volunteer,
    round(100.0 * ccis_requirement_met_1to5 / nullif(ccis, 0), 1) as pct_ccis_requirement_met_1to5,
    round(100.0 * ccis_requirement_met_2to5 / nullif(ccis, 0), 1) as pct_ccis_requirement_met_2to5,
    round(100.0 * circles_started / nullif(mentoring_circles, 0), 1) as pct_circles_started,
    round(100.0 * ccis_sessions_started / nullif(ccis, 0), 1) as pct_ccis_sessions_started,
    round(100.0 * ccis_all_circles_started / nullif(ccis, 0), 1) as pct_ccis_all_circles_started
from with_all w
