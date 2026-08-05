{{ config(materialized='table') }}

-- prod_e2_volunteer_consistency: per-volunteer session consistency, dashboard-ready
-- Grain: one row per (volunteer_id, chapter_id, academic_year)
-- Built on fct_e2_volunteer_consistency (marts/core) + dim_chapter_mapping (marts/core), for the
-- standalone volunteer consistency dashboard. Deliberately a sibling of prod_e2_dashboard_summary,
-- not a dependency of it -- prod_e2_dashboard_summary reads fct_e2_volunteer_consistency directly,
-- so a change here never affects it.
-- chapter_name comes from dim_chapter_mapping, not fct_e2_volunteer_consistency's own chapter_name
-- (sourced from int_bubble__partner) -- the two disagree for a real subset of chapters, and
-- prod_e2_dashboard_summary already shows the dim_chapter_mapping version, so this keeps the same
-- chapter label consistent across all three dashboards.

select
    vc.volunteer_id,
    vc.volunteer_name,
    vc.chapter_id,
    cd.chapter_name,
    cd.city_name,
    cd.co_name,
    cd.cho_name,
    cd.engine,
    cd.chapter_status,
    vc.academic_year,
    vc.is_active,
    vc.planned_sessions,
    vc.attended_sessions,
    vc.original_sessions,
    vc.substitute_sessions,
    vc.hours_contributed,
    vc.attendance_pct,
    vc.consistency_status
from {{ ref('fct_e2_volunteer_consistency') }} vc
left join {{ ref('dim_chapter_mapping') }} cd
    on vc.chapter_id::text = cd.chapter_id
