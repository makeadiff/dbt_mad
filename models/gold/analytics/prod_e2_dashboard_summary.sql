{{ config(materialized='table') }}

-- prod_e2_dashboard_summary: chapter x academic-year rollup of session delivery, coverage gaps, and
-- volunteer/child consistency, for the chapter ops dashboard
-- Grain: one row per (chapter_id, academic_year)
-- Ported from the legacy fct_dashboard_summary model, rebuilt entirely on marts/core facts and
-- dimensions (fct_e2_sessions_summary, fct_e2_school_coverage, fct_e2_volunteer_consistency,
-- fct_e2_child_consistency, fct_e2_volunteer_recruitment, dim_chapter_mapping) per this project's
-- gold/analytics layering rule.

with volunteer_metrics as (
    select
        chapter_id::text as chapter_id,
        academic_year,
        count(distinct volunteer_id) as total_volunteers,
        count(*) filter (where consistency_status = 'Healthy') as volunteers_healthy,
        count(*) filter (where consistency_status = 'At Risk') as volunteers_at_risk,
        count(*) filter (where consistency_status = 'Unhealthy') as volunteers_unhealthy,
        count(*) filter (where consistency_status = 'No Sessions Yet') as volunteers_no_sessions
    from {{ ref('fct_e2_volunteer_consistency') }}
    group by chapter_id::text, academic_year
),

child_metrics as (
    select
        chapter_id::text as chapter_id,
        academic_year,
        count(distinct child_id) as total_children_in_slots,
        count(*) filter (where consistency_status = 'Healthy') as children_healthy,
        count(*) filter (where consistency_status = 'At Risk') as children_at_risk,
        count(*) filter (where consistency_status = 'Unhealthy') as children_unhealthy,
        count(*) filter (where consistency_status = 'No Sessions Yet') as children_no_sessions
    from {{ ref('fct_e2_child_consistency') }}
    group by chapter_id::text, academic_year
)

select
    ss.chapter_id,
    ss.chapter_name,
    ss.city_name,
    ss.state,
    ss.co_name,
    cd.cho_name,
    ss.engine,
    ss.chapter_status,
    ss.academic_year,
    ss.total_planned_sessions,
    ss.total_sessions_happened,
    ss.total_original_sessions,
    ss.total_substitute_sessions,
    ss.total_cancellations,
    ss.total_absenteeism,
    ss.pct_sessions_happened,
    ss.pct_original_sessions,
    ss.pct_substitute_sessions,
    ss.pct_cancellations,
    cbs.is_chapter_active,
    coalesce(cbs.total_sections, 0) as total_sections,
    coalesce(cbs.total_slots, 0) as total_slots,
    coalesce(cbs.total_classes, 0) as total_classes,
    coalesce(cbs.sections_without_volunteer, 0) as sections_without_volunteer,
    coalesce(cbs.children_without_mentor, 0) as children_without_mentor,
    coalesce(cbs.total_children_with_mentor, 0) as total_children_with_mentor,
    coalesce(cbs.total_volunteers_assigned, 0) as total_volunteers_assigned,
    coalesce(cbs.classes_with_more_than_1_volunteer, 0) as classes_with_more_than_1_volunteer,
    coalesce(vr.total_volunteers_in_school, 0) as total_volunteers_in_school,
    coalesce(vm.total_volunteers, 0) as total_volunteers,
    coalesce(vm.volunteers_healthy, 0) as volunteers_healthy,
    coalesce(vm.volunteers_at_risk, 0) as volunteers_at_risk,
    coalesce(vm.volunteers_unhealthy, 0) as volunteers_unhealthy,
    coalesce(vm.volunteers_no_sessions, 0) as volunteers_no_sessions,
    round(vm.volunteers_healthy * 100.0 / nullif(vm.total_volunteers, 0), 1) as pct_volunteers_consistent,
    round(vm.volunteers_at_risk * 100.0 / nullif(vm.total_volunteers, 0), 1) as pct_volunteers_at_risk,
    round(vm.volunteers_unhealthy * 100.0 / nullif(vm.total_volunteers, 0), 1) as pct_volunteers_unhealthy,
    round(vm.volunteers_no_sessions * 100.0 / nullif(vm.total_volunteers, 0), 1) as pct_volunteers_no_sessions,
    cm.total_children_in_slots,
    cm.children_healthy,
    cm.children_at_risk,
    cm.children_unhealthy,
    cm.children_no_sessions,
    round(cm.children_healthy * 100.0 / nullif(cm.total_children_in_slots, 0), 1) as pct_children_consistent,
    round(cm.children_at_risk * 100.0 / nullif(cm.total_children_in_slots, 0), 1) as pct_children_at_risk,
    round(cm.children_unhealthy * 100.0 / nullif(cm.total_children_in_slots, 0), 1) as pct_children_unhealthy,
    round(cm.children_no_sessions * 100.0 / nullif(cm.total_children_in_slots, 0), 1) as pct_children_no_sessions,
    cbs.total_children_in_system
from {{ ref('fct_e2_sessions_summary') }} ss
left join {{ ref('dim_chapter_mapping') }} cd
    on ss.chapter_id = cd.chapter_id
left join {{ ref('fct_e2_school_coverage') }} cbs
    on ss.chapter_id = cbs.chapter_id
    and ss.academic_year = cbs.academic_year
left join volunteer_metrics vm
    on ss.chapter_id = vm.chapter_id
    and ss.academic_year = vm.academic_year
left join child_metrics cm
    on ss.chapter_id = cm.chapter_id
    and ss.academic_year = cm.academic_year
left join {{ ref('fct_e2_volunteer_recruitment') }} vr
    on ss.chapter_id = vr.chapter_id
    and ss.academic_year = vr.academic_year
