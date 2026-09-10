{{ config(materialized='table') }}

-- prod_e1_dashboard_status_mix: long/unpivoted status breakdowns for pie, donut, and 100%-stacked-bar
-- charts in Superset, for E1 (session-ops / platform_commons). E1 counterpart to
-- prod_e2_dashboard_status_mix.
-- Grain: one row per (school_id, academic_year, metric_group, category)
-- Deliberately a sibling of prod_e1_dashboard_summary, not a dependency of it -- built straight on
-- the same marts/core facts (fct_e1_school_coverage, fct_e1_session_summary,
-- fct_e1_cancellation_reasons, dim_pc_school), per this project's gold/analytics-builds-on-marts-only
-- convention.
--
-- Missing vs. prod_e2_dashboard_status_mix:
-- - 'Volunteer Consistency' / 'Child Consistency': no fct_e1 equivalent of
--   fct_e2_volunteer_consistency/fct_e2_child_consistency exists yet.
-- 'Cancellation Reason' category is platform_commons' raw requesting_reason text (see
-- fct_e1_cancellation_reasons for detail) -- passed through as-is, same as E2's holiday_reason.
-- 'Session Delivery' (Planned/Happened) and 'Session Not Happened Breakdown' (Cancelled/Absent) both
-- pull total_planned_sessions/total_absenteeism from fct_e1_session_summary -- a coarser, assumed-
-- sessions-per-week estimate rather than E2's real per-section schedule math (platform_commons has
-- no schedule-start signal to compute the real thing from). See that model's header for the formula
-- and its known overestimation tradeoff before reading these two metric_groups as precise.

with session_happened_breakdown_mix as (
    select school_id, academic_year, 'Session Happened Breakdown' as metric_group, 'Original Session' as category, total_original_sessions as count
    from {{ ref('fct_e1_session_summary') }}
    union all
    select school_id, academic_year, 'Session Happened Breakdown', 'Substitute Session', total_substitute_sessions
    from {{ ref('fct_e1_session_summary') }}
),

cancellation_reason_mix as (
    select
        school_id,
        academic_year,
        'Cancellation Reason' as metric_group,
        cancellation_reason as category,
        cancelled_sessions_count as count
    from {{ ref('fct_e1_cancellation_reasons') }}
),

-- Not a partition -- Happened is a subset of Planned, not its complement -- meant for a grouped
-- bar comparing Planned vs. Happened per school, not a pie or stacked bar. Same caveat as E2's
-- session_delivery_mix.
session_delivery_mix as (
    select school_id, academic_year, 'Session Delivery' as metric_group, 'Planned' as category, total_planned_sessions as count
    from {{ ref('fct_e1_session_summary') }}
    union all
    select school_id, academic_year, 'Session Delivery', 'Happened', total_sessions_happened
    from {{ ref('fct_e1_session_summary') }}
),

session_not_happened_breakdown_mix as (
    select school_id, academic_year, 'Session Not Happened Breakdown' as metric_group, 'Cancelled' as category, total_cancellations as count
    from {{ ref('fct_e1_session_summary') }}
    union all
    select school_id, academic_year, 'Session Not Happened Breakdown', 'Absent', total_absenteeism
    from {{ ref('fct_e1_session_summary') }}
),

mentor_coverage_mix as (
    select school_id, academic_year, 'Mentor Coverage' as metric_group, 'With Mentor' as category, total_children_with_mentor as count
    from {{ ref('fct_e1_school_coverage') }}
    union all
    select school_id, academic_year, 'Mentor Coverage', 'Without Mentor', children_without_mentor
    from {{ ref('fct_e1_school_coverage') }}
),

section_volunteer_coverage_mix as (
    select school_id, academic_year, 'Section Volunteer Coverage' as metric_group, 'Sections With Volunteer' as category, greatest(total_sections - sections_without_volunteer, 0) as count
    from {{ ref('fct_e1_school_coverage') }}
    union all
    select school_id, academic_year, 'Section Volunteer Coverage', 'Sections Without Volunteer', sections_without_volunteer
    from {{ ref('fct_e1_school_coverage') }}
),

unpivoted as (
    select * from session_happened_breakdown_mix
    union all
    select * from cancellation_reason_mix
    union all
    select * from session_delivery_mix
    union all
    select * from session_not_happened_breakdown_mix
    union all
    select * from mentor_coverage_mix
    union all
    select * from section_volunteer_coverage_mix
)

select
    u.school_id,
    sch.school_name,
    sch.city_name,
    sch.state_name,
    sch.is_active as is_school_active,
    u.academic_year,
    u.metric_group,
    u.category,
    u.count
from unpivoted u
left join {{ ref('dim_pc_school') }} sch
    on u.school_id = sch.school_id
