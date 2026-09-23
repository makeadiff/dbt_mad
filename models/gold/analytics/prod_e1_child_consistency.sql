{{ config(materialized='table') }}

-- prod_e1_child_consistency: per-child session consistency, dashboard-ready, for E1
-- (session-ops / platform_commons). E1 counterpart to prod_e2_child_consistency.
-- Grain: one row per (student_id, school_id, academic_year)
-- Built on fct_e1_child_consistency (marts/core) + dim_pc_school (marts/core), for the standalone
-- E1 child consistency dashboard. Deliberately a sibling of prod_e1_dashboard_summary, not a
-- dependency of it -- prod_e1_dashboard_summary reads fct_e1_child_consistency directly, so a
-- change here never affects it.

select
    cc.student_id,
    cc.student_name,
    cc.school_id,
    sch.school_name,
    sch.city_name,
    sch.state_name,
    cc.academic_year,
    cc.sessions_happened,
    cc.attended_sessions,
    cc.hours_of_support,
    coalesce(cc.attendance_pct, 0) as attendance_pct,
    cc.consistency_status
from {{ ref('fct_e1_child_consistency') }} cc
left join {{ ref('dim_pc_school') }} sch
    on cc.school_id = sch.school_id
