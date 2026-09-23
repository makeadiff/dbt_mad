{{ config(materialized='table') }}

-- prod_e1_volunteer_consistency: per-volunteer session consistency, dashboard-ready, for E1
-- (session-ops / platform_commons). E1 counterpart to prod_e2_volunteer_consistency.
-- Grain: one row per (volunteer_id, school_id, academic_year)
-- Built on fct_e1_volunteer_consistency (marts/core) + dim_pc_school (marts/core), for the
-- standalone E1 volunteer consistency dashboard. Deliberately a sibling of prod_e1_dashboard_summary,
-- not a dependency of it -- prod_e1_dashboard_summary reads fct_e1_volunteer_consistency directly,
-- so a change here never affects it.
-- No chapter_status/co_name/cho_name/engine here -- platform_commons has no CO/CHO assignment or
-- engine concept, same reasoning prod_e1_dashboard_summary already documents for school-level
-- metrics.

select
    vc.volunteer_id,
    vc.volunteer_name,
    vc.school_id,
    sch.school_name,
    sch.city_name,
    sch.state_name,
    vc.academic_year,
    vc.is_active,
    vc.planned_sessions,
    vc.attended_sessions,
    vc.original_sessions,
    vc.substitute_sessions,
    vc.hours_contributed,
    vc.attendance_pct,
    vc.consistency_status
from {{ ref('fct_e1_volunteer_consistency') }} vc
left join {{ ref('dim_pc_school') }} sch
    on vc.school_id = sch.school_id
