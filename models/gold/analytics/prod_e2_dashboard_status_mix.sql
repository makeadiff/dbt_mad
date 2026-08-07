{{ config(materialized='table') }}

-- prod_e2_dashboard_status_mix: long/unpivoted status breakdowns for pie, donut, and 100%-stacked-bar
-- charts in Superset.
-- Grain: one row per (chapter_id, academic_year, metric_group, category)
-- prod_e2_dashboard_summary is wide -- one column per category (volunteers_healthy, volunteers_at_risk,
-- ...) -- but Superset's pie/stacked charts need one dimension column (category) + one metric column
-- (count) instead. This model unpivots every wide status breakdown into that shape, one metric_group
-- per breakdown, so a chart just filters to a metric_group and sets category as Dimension, count as Metric.
-- Deliberately a sibling of prod_e2_dashboard_summary, not a dependency of it -- built straight on the
-- same marts/core facts (fct_e2_volunteer_consistency, fct_e2_child_consistency, fct_e2_sessions_summary,
-- fct_e2_school_coverage), so a change here never affects it, per this project's
-- gold/analytics-builds-on-marts-only convention.

with volunteer_consistency_mix as (
    select
        chapter_id::text as chapter_id,
        academic_year,
        'Volunteer Consistency' as metric_group,
        consistency_status as category,
        count(distinct volunteer_id) as count
    from {{ ref('fct_e2_volunteer_consistency') }}
    group by chapter_id::text, academic_year, consistency_status
),

child_consistency_mix as (
    select
        chapter_id::text as chapter_id,
        academic_year,
        'Child Consistency' as metric_group,
        consistency_status as category,
        count(distinct child_id) as count
    from {{ ref('fct_e2_child_consistency') }}
    group by chapter_id::text, academic_year, consistency_status
),

session_happened_breakdown_mix as (
    select chapter_id, academic_year, 'Session Happened Breakdown' as metric_group, 'Original Session' as category, total_original_sessions as count
    from {{ ref('fct_e2_sessions_summary') }}
    union all
    select chapter_id, academic_year, 'Session Happened Breakdown', 'Substitute Session', total_substitute_sessions
    from {{ ref('fct_e2_sessions_summary') }}
),

session_not_happened_breakdown_mix as (
    select chapter_id, academic_year, 'Session Not Happened Breakdown' as metric_group, 'Cancelled' as category, total_cancellations as count
    from {{ ref('fct_e2_sessions_summary') }}
    union all
    select chapter_id, academic_year, 'Session Not Happened Breakdown', 'Absent', total_absenteeism
    from {{ ref('fct_e2_sessions_summary') }}
),

-- Planned and Happened are not a partition (Happened is a subset of Planned, not its complement) --
-- unlike every other metric_group here, these two don't sum to a clean whole. Meant for a grouped
-- bar comparing Planned vs. Happened per chapter, not a pie or stacked bar.
session_delivery_mix as (
    select chapter_id, academic_year, 'Session Delivery' as metric_group, 'Planned' as category, total_planned_sessions as count
    from {{ ref('fct_e2_sessions_summary') }}
    union all
    select chapter_id, academic_year, 'Session Delivery', 'Happened', total_sessions_happened
    from {{ ref('fct_e2_sessions_summary') }}
),

mentor_coverage_mix as (
    select chapter_id, academic_year, 'Mentor Coverage' as metric_group, 'With Mentor' as category, total_children_with_mentor as count
    from {{ ref('fct_e2_school_coverage') }}
    union all
    select chapter_id, academic_year, 'Mentor Coverage', 'Without Mentor', children_without_mentor
    from {{ ref('fct_e2_school_coverage') }}
),

section_volunteer_coverage_mix as (
    select chapter_id, academic_year, 'Section Volunteer Coverage' as metric_group, 'Sections With Volunteer' as category, greatest(total_sections - sections_without_volunteer, 0) as count
    from {{ ref('fct_e2_school_coverage') }}
    union all
    select chapter_id, academic_year, 'Section Volunteer Coverage', 'Sections Without Volunteer', sections_without_volunteer
    from {{ ref('fct_e2_school_coverage') }}
),

unpivoted as (
    select * from volunteer_consistency_mix
    union all
    select * from child_consistency_mix
    union all
    select * from session_happened_breakdown_mix
    union all
    select * from session_not_happened_breakdown_mix
    union all
    select * from session_delivery_mix
    union all
    select * from mentor_coverage_mix
    union all
    select * from section_volunteer_coverage_mix
)

select
    u.chapter_id,
    cd.chapter_name,
    cd.city_name,
    cd.co_name,
    cd.cho_name,
    cd.engine,
    case when ccs.is_currently_active then 'Active' else 'Inactive' end as chapter_status,
    u.academic_year,
    u.metric_group,
    u.category,
    u.count
from unpivoted u
left join {{ ref('dim_chapter_mapping') }} cd
    on u.chapter_id = cd.chapter_id
left join {{ ref('dim_chapter_current_status') }} ccs
    on u.chapter_id = ccs.school_id::text
