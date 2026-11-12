{{ config(materialized='table') }}

-- CCI dashboard (E1) district rollup, snake_case: one row per district (spec E1 §7 D1-D3) plus an
-- "All districts" row that is the sum of the chapters, because Dalgo can't total a table. Feeds the
-- district table and goals table (spec §3.8, §3.9). Counts are summed from prod_cci_chapter_summary;
-- every % is a ratio of sums, never an average of chapter %s. One DO per district
-- (seed_cci_chapter_group), so do_name rides along; the All districts row has none.
-- district_sort is the district number (8-11), with All districts last (12), as
-- prod_do_district_summary does ("All districts" would sort first by name).
--
-- "CCIs" counts only CCIs with children this year (27, not 28), so the CCI percentages use the same
-- denominator as the spec's national figures. Sarada Niketan (no children) sits in District 9 and
-- adds no children.
-- Layout v4 (spec §7 L1-L7): setup complete, weekly slots, minimum volunteers, planned-based
-- sessions and leads from CCI links (additive). City and open-pool leads aren't per CCI, so they
-- live on prod_cci_national_display only. The All districts row is the sum of the district rows.
-- Baseline (spec §7 B1-B5): summed from prod_cci_baseline_chapters, joined on district; % with a
-- baseline is a ratio of sums over active children.
--
-- Grain: one row per district, plus 'All districts'.

with grouped as (
    select
        district,
        do_name,
        substring(district from '[0-9]+')::int as district_sort,
        count(*) filter (where has_children) as ccis,
        sum(active_children) as active_children,
        sum(children_with_volunteer) as children_with_volunteer,
        sum(children_not_in_circle) as children_not_in_circle,
        sum(children_in_circle_no_slot) as children_in_circle_no_slot,
        sum(children_in_circle_no_volunteer) as children_in_circle_no_volunteer,
        sum(children_foundations) as children_foundations,
        sum(children_education_support) as children_education_support,
        sum(children_transition_readiness) as children_transition_readiness,
        sum(children_programme_not_set) as children_programme_not_set,
        count(*) filter (where setup_complete) as ccis_setup_complete,
        sum(sections_no_circle) as sections_no_circle,
        sum(weekly_session_slots) as weekly_session_slots,
        sum(volunteers_needed_min_1to5) as volunteers_needed_min_1to5,
        sum(volunteers_needed_min_2to5) as volunteers_needed_min_2to5,
        sum(volunteers_minimum_to_start) as volunteers_minimum_to_start,
        sum(volunteers_needed_as_set_up_1to5) as volunteers_needed_as_set_up_1to5,
        sum(volunteers_needed_as_set_up_2to5) as volunteers_needed_as_set_up_2to5,
        sum(volunteers_placed) as volunteers_placed,
        sum(leads_chapter_link) as leads_chapter_link,
        sum(mentoring_circles) as mentoring_circles,
        sum(circles_no_volunteer) as circles_no_volunteer,
        sum(circles_one_volunteer) as circles_one_volunteer,
        sum(circles_two_plus_volunteers) as circles_two_plus_volunteers,
        sum(circles_needing_volunteer) as circles_needing_volunteer,
        sum(circles_not_started) as circles_not_started,
        sum(circles_stopped_logging) as circles_stopped_logging,
        sum(circles_with_weekly_slot) as circles_with_weekly_slot,
        sum(circles_no_weekly_slot) as circles_no_weekly_slot,
        count(*) filter (where volunteer_requirement_met_1to5) as ccis_requirement_met_1to5,
        count(*) filter (where volunteer_requirement_met_2to5) as ccis_requirement_met_2to5,
        sum(circles_started) as circles_started,
        count(*) filter (where has_sessions_started) as ccis_sessions_started,
        count(*) filter (where all_circles_started) as ccis_all_circles_started,
        sum(sessions_planned_since_1_oct) as sessions_planned_since_1_oct,
        sum(sessions_held_since_1_oct) as sessions_held_since_1_oct,
        sum(sessions_held_by_substitute_since_1_oct) as sessions_held_by_substitute_since_1_oct,
        sum(sessions_cancelled_since_1_oct) as sessions_cancelled_since_1_oct,
        sum(sessions_volunteer_absent_since_1_oct) as sessions_volunteer_absent_since_1_oct,
        sum(sessions_no_volunteer_assigned_since_1_oct) as sessions_no_volunteer_assigned_since_1_oct
    from {{ ref('prod_cci_chapter_summary') }}
    group by 1, 2
),

all_row as (
    select
        'All districts'::text as district,
        null::text as do_name,
        12 as district_sort,
        sum(ccis), sum(active_children), sum(children_with_volunteer), sum(children_not_in_circle),
        sum(children_in_circle_no_slot), sum(children_in_circle_no_volunteer), sum(children_foundations),
        sum(children_education_support), sum(children_transition_readiness), sum(children_programme_not_set),
        sum(ccis_setup_complete), sum(sections_no_circle), sum(weekly_session_slots),
        sum(volunteers_needed_min_1to5), sum(volunteers_needed_min_2to5), sum(volunteers_minimum_to_start),
        sum(volunteers_needed_as_set_up_1to5), sum(volunteers_needed_as_set_up_2to5),
        sum(volunteers_placed), sum(leads_chapter_link), sum(mentoring_circles), sum(circles_no_volunteer),
        sum(circles_one_volunteer), sum(circles_two_plus_volunteers), sum(circles_needing_volunteer),
        sum(circles_not_started), sum(circles_stopped_logging), sum(circles_with_weekly_slot),
        sum(circles_no_weekly_slot), sum(ccis_requirement_met_1to5), sum(ccis_requirement_met_2to5),
        sum(circles_started), sum(ccis_sessions_started), sum(ccis_all_circles_started),
        sum(sessions_planned_since_1_oct), sum(sessions_held_since_1_oct),
        sum(sessions_held_by_substitute_since_1_oct), sum(sessions_cancelled_since_1_oct),
        sum(sessions_volunteer_absent_since_1_oct), sum(sessions_no_volunteer_assigned_since_1_oct)
    from grouped
),

with_all as (
    select * from grouped
    union all
    select * from all_row
),

baseline as (
    select
        coalesce(district, 'All districts')         as district,
        sum(children_with_baseline)                 as children_with_baseline,
        sum(children_cleared_aser)                  as children_cleared_aser,
        sum(baseline_forms_submitted)               as baseline_forms_submitted
    from {{ ref('prod_cci_baseline_chapters') }}
    group by rollup (district)
)

select
    w.*,
    round(100.0 * children_with_volunteer / nullif(active_children, 0), 1) as pct_children_with_volunteer,
    round(100.0 * ccis_setup_complete / nullif(ccis, 0), 1) as pct_ccis_setup_complete,
    round(100.0 * ccis_requirement_met_1to5 / nullif(ccis, 0), 1) as pct_ccis_requirement_met_1to5,
    round(100.0 * ccis_requirement_met_2to5 / nullif(ccis, 0), 1) as pct_ccis_requirement_met_2to5,
    round(100.0 * circles_started / nullif(mentoring_circles, 0), 1) as pct_circles_started,
    round(100.0 * ccis_sessions_started / nullif(ccis, 0), 1) as pct_ccis_sessions_started,
    round(100.0 * ccis_all_circles_started / nullif(ccis, 0), 1) as pct_ccis_all_circles_started,
    sessions_held_since_1_oct - sessions_held_by_substitute_since_1_oct as sessions_held_by_original_since_1_oct,
    round(100.0 * (sessions_held_since_1_oct - sessions_held_by_substitute_since_1_oct)
        / nullif(sessions_planned_since_1_oct, 0), 1) as pct_held_by_original_since_1_oct,
    round(100.0 * sessions_held_since_1_oct / nullif(sessions_planned_since_1_oct, 0), 1) as pct_sessions_held_since_1_oct,
    round(100.0 * sessions_cancelled_since_1_oct / nullif(sessions_planned_since_1_oct, 0), 1) as pct_sessions_cancelled_since_1_oct,
    coalesce(bl.children_with_baseline, 0) as children_with_baseline,
    round(100.0 * coalesce(bl.children_with_baseline, 0) / nullif(w.active_children, 0), 1) as pct_children_with_baseline,
    coalesce(bl.children_cleared_aser, 0) as children_cleared_aser,
    coalesce(bl.baseline_forms_submitted, 0) as baseline_forms_submitted
from with_all w
left join baseline bl on w.district = bl.district
