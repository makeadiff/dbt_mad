{{ config(materialized='table') }}

-- prod_e1_dashboard_summary: school x academic-year rollup of coverage gaps for the E1
-- (session-ops / platform_commons) dashboard. E1 counterpart to prod_e2_dashboard_summary,
-- built the same way -- on top of marts/core facts only, per this project's gold/analytics
-- layering rule.
-- Grain: one row per (school_id, academic_year)
--
-- Now also joins in fct_e1_session_summary (session delivery: happened/original/substitute counts
-- and their pct's, cancellations, plus total_planned_sessions/total_absenteeism/pct_sessions_happened/
-- pct_cancellations) -- E1's counterpart to E2's fct_e2_sessions_summary contribution. The planned/
-- absenteeism side is a coarser estimate than E2's -- see fct_e1_session_summary's header for the
-- fixed-window-and-assumed-sessions-per-week approach and its known overestimation tradeoff.
-- Now also joins in dim_pc_school for school_name/city_name/state_name (E1's counterpart to E2's
-- dim_chapter_mapping name/location enrichment). city_name/state_name are null for schools whose
-- name doesn't match a platform_commons CENTER worknode -- currently 33 of 82 schools resolve; see
-- dim_pc_school's header for why. co_name/cho_name/engine have no E1 equivalent (no CO/CHO
-- assignment or engine concept exists in platform_commons) and total_volunteers_in_school is now
-- sourced from fct_e1_school_coverage directly (all-time distinct tagged volunteers per school; see
-- that model's header for how this differs from E2's recruitment-bucket-based version).
-- Now also joins in fct_e1_volunteer_consistency/fct_e1_child_consistency for the
-- volunteers_healthy/at_risk/unhealthy/no_sessions and children_healthy/at_risk/unhealthy/no_sessions
-- breakdowns -- E1's counterpart to prod_e2_dashboard_summary's volunteer_metrics/child_metrics.
-- See fct_e1_volunteer_consistency's header for its planned_sessions estimation tradeoff (no
-- per-volunteer allocation start date in platform_commons, unlike E2).

with volunteer_metrics as (
    select
        school_id,
        academic_year,
        count(distinct volunteer_id) as total_volunteers,
        count(*) filter (where consistency_status = 'Healthy') as volunteers_healthy,
        count(*) filter (where consistency_status = 'At Risk') as volunteers_at_risk,
        count(*) filter (where consistency_status = 'Unhealthy') as volunteers_unhealthy,
        count(*) filter (where consistency_status = 'No Sessions Yet') as volunteers_no_sessions
    from {{ ref('fct_e1_volunteer_consistency') }}
    group by school_id, academic_year
),

child_metrics as (
    select
        school_id,
        academic_year,
        count(distinct student_id) as total_children,
        count(*) filter (where consistency_status = 'Healthy') as children_healthy,
        count(*) filter (where consistency_status = 'At Risk') as children_at_risk,
        count(*) filter (where consistency_status = 'Unhealthy') as children_unhealthy,
        count(*) filter (where consistency_status = 'No Sessions Yet') as children_no_sessions
    from {{ ref('fct_e1_child_consistency') }}
    group by school_id, academic_year
)

select
    cbs.school_id,
    sch.school_name,
    sch.city_name,
    sch.state_name,
    cbs.academic_year,
    cbs.is_school_active,
    coalesce(cbs.total_sections, 0) as total_sections,
    coalesce(cbs.total_slots, 0) as total_slots,
    coalesce(cbs.total_classes, 0) as total_classes,
    coalesce(cbs.total_children_in_system, 0) as total_children_in_system,
    coalesce(cbs.total_children_with_mentor, 0) as total_children_with_mentor,
    coalesce(cbs.children_without_mentor, 0) as children_without_mentor,
    coalesce(cbs.sections_without_volunteer, 0) as sections_without_volunteer,
    coalesce(cbs.total_volunteers_assigned, 0) as total_volunteers_assigned,
    coalesce(cbs.total_volunteers_in_school, 0) as total_volunteers_in_school,
    coalesce(cbs.classes_with_more_than_1_volunteer, 0) as classes_with_more_than_1_volunteer,
    coalesce(cbs.classes_started, 0) as classes_started,
    coalesce(cbs.classes_not_started, 0) as classes_not_started,
    ss.total_planned_sessions,
    coalesce(ss.total_sessions_happened, 0) as total_sessions_happened,
    coalesce(ss.total_original_sessions, 0) as total_original_sessions,
    coalesce(ss.total_substitute_sessions, 0) as total_substitute_sessions,
    ss.total_absenteeism,
    ss.pct_sessions_happened,
    ss.pct_original_sessions,
    ss.pct_substitute_sessions,
    ss.pct_cancellations,
    coalesce(ss.total_cancellations, 0) as total_cancellations,
    ss.cancellation_reasons,
    coalesce(vm.total_volunteers, 0) as consistency_total_volunteers,
    coalesce(vm.volunteers_healthy, 0) as volunteers_healthy,
    coalesce(vm.volunteers_at_risk, 0) as volunteers_at_risk,
    coalesce(vm.volunteers_unhealthy, 0) as volunteers_unhealthy,
    coalesce(vm.volunteers_no_sessions, 0) as volunteers_no_sessions,
    coalesce(cm.total_children, 0) as consistency_total_children,
    coalesce(cm.children_healthy, 0) as children_healthy,
    coalesce(cm.children_at_risk, 0) as children_at_risk,
    coalesce(cm.children_unhealthy, 0) as children_unhealthy,
    coalesce(cm.children_no_sessions, 0) as children_no_sessions
from {{ ref('fct_e1_school_coverage') }} cbs
left join {{ ref('dim_pc_school') }} sch
    on cbs.school_id = sch.school_id
left join {{ ref('fct_e1_session_summary') }} ss
    on cbs.school_id = ss.school_id
    and cbs.academic_year = ss.academic_year
left join volunteer_metrics vm
    on cbs.school_id = vm.school_id
    and cbs.academic_year = vm.academic_year
left join child_metrics cm
    on cbs.school_id = cm.school_id
    and cbs.academic_year = cm.academic_year
