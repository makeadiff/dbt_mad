{{ config(materialized='table') }}

-- prod_e2_child_consistency: per-child session consistency, dashboard-ready
-- Grain: one row per (child_id, chapter_id, academic_year)
-- Built on fct_e2_child_consistency (marts/core) + dim_chapter_mapping (marts/core), for the
-- standalone child consistency dashboard. Deliberately a sibling of prod_e2_dashboard_summary,
-- not a dependency of it -- prod_e2_dashboard_summary reads fct_e2_child_consistency directly,
-- so a change here never affects it.
-- chapter_name comes from dim_chapter_mapping, not fct_e2_child_consistency's own chapter_name
-- (sourced from int_bubble__partner) -- the two disagree for a real subset of chapters, and
-- prod_e2_dashboard_summary already shows the dim_chapter_mapping version, so this keeps the same
-- chapter label consistent across all three dashboards.

select
    cc.child_id,
    cc.child_first_name,
    cc.child_last_name,
    cc.chapter_id,
    cd.chapter_name,
    cd.city_name,
    cd.co_name,
    cd.cho_name,
    cd.engine,
    cd.chapter_status,
    cc.academic_year,
    cc.sessions_happened,
    cc.attended_sessions,
    cc.hours_of_support,
    cc.attendance_pct,
    cc.consistency_status
from {{ ref('fct_e2_child_consistency') }} cc
left join {{ ref('dim_chapter_mapping') }} cd
    on cc.chapter_id::text = cd.chapter_id
