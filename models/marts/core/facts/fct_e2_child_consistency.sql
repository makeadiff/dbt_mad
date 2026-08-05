{{ config(materialized='table') }}

-- fct_e2_child_consistency: per-child session consistency (sessions happened vs. attended) for one academic year
-- Grain: one row per (child_id, chapter_id, academic_year)
-- Ported from the legacy fct_child_consistency model, rebuilt on fct_e2_child_allocation_history +
-- fct_e2_child_attendance_by_slot_date + fct_e2_volunteer_attendance_by_slot_date (for the count of
-- sessions that actually happened at a section, independent of any one child's own attendance).
-- Stays in marts/core (not gold/analytics) since it's consumed by more than one thing: rolled up into
-- prod_e2_dashboard_summary, and queried directly at row-level for a standalone child consistency
-- dashboard -- exactly the "reusable business fact" case the shared marts layer exists for.

with sessions_happened_per_section as (
    select
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) as sessions_happened
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    group by slot_class_section_id, academic_year
),

child_attendance_agg as (
    select
        child_id,
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) as attended_sessions
    from {{ ref('fct_e2_child_attendance_by_slot_date') }}
    group by child_id, slot_class_section_id, academic_year
),

joined as (
    select
        ca.child_id,
        ca.child_first_name,
        ca.child_last_name,
        ca.partner_id as chapter_id,
        ca.partner_name as chapter_name,
        ca.academic_year,
        coalesce(sh.sessions_happened, 0) as sessions_happened,
        coalesce(aa.attended_sessions, 0) as attended_sessions
    from {{ ref('fct_e2_child_allocation_history') }} ca
    left join sessions_happened_per_section sh
        on ca.slot_class_section_id = sh.slot_class_section_id
        and ca.academic_year = sh.academic_year
    left join child_attendance_agg aa
        on ca.child_id = aa.child_id
        and ca.slot_class_section_id = aa.slot_class_section_id
        and ca.academic_year = aa.academic_year
),

child_aggregated as (
    select
        child_id,
        child_first_name,
        child_last_name,
        chapter_id,
        chapter_name,
        academic_year,
        sum(sessions_happened) as sessions_happened,
        sum(attended_sessions) as attended_sessions
    from joined
    group by child_id, child_first_name, child_last_name, chapter_id, chapter_name, academic_year
)

select
    child_id,
    child_first_name,
    child_last_name,
    chapter_id,
    chapter_name,
    academic_year,
    sessions_happened,
    attended_sessions,
    -- TODO: update multiplier when actual session duration per slot is available.
    attended_sessions * 2 as hours_of_support,
    case
        when sessions_happened = 0 then null
        else round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1)
    end as attendance_pct,
    case
        when sessions_happened = 0 then 'No Sessions Yet'
        when round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1) >= 90 then 'Healthy'
        when round(attended_sessions::numeric / nullif(sessions_happened, 0) * 100, 1) >= 75 then 'At Risk'
        else 'Unhealthy'
    end as consistency_status
from child_aggregated
order by chapter_id, consistency_status
