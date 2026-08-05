{{ config(materialized='table') }}

-- fct_e2_volunteer_consistency: per-volunteer session consistency (planned vs. attended) for one academic year
-- Grain: one row per (volunteer_id, chapter_id, academic_year)
-- Ported from the legacy fct_volunteer_consistency model, rebuilt on
-- fct_e2_volunteer_allocation_history + fct_e2_volunteer_attendance_by_slot_date + fct_e2_cancellations.
-- Stays in marts/core (not gold/analytics) since it's consumed by more than one thing: rolled up into
-- prod_e2_dashboard_summary, and queried directly at row-level for a standalone volunteer consistency
-- dashboard -- exactly the "reusable business fact" case the shared marts layer exists for.

with attendance_agg as (
    select
        volunteer_id,
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) as attended_sessions,
        count(distinct date_of_slot) filter (where is_substitute = false) as original_sessions,
        count(distinct date_of_slot) filter (where is_substitute = true) as substitute_sessions
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    group by volunteer_id, slot_class_section_id, academic_year
),

allocation_with_dow as (
    select
        *,
        case trim(day_of_week)
            when 'Sunday' then 0
            when 'Monday' then 1
            when 'Tuesday' then 2
            when 'Wednesday' then 3
            when 'Thursday' then 4
            when 'Friday' then 5
            when 'Saturday' then 6
            else null
        end as slot_dow
    from {{ ref('fct_e2_volunteer_allocation_history') }}
),

planned_sessions_calc as (
    select
        *,
        greatest(slot_class_sec_start_date::date, sessions_start_date::date) as effective_start,
        case
            when is_active then least(
                current_date,
                coalesce(
                    slot_class_sec_end_date::date,
                    case
                        when academic_year = '2025-2026' then '2026-04-30'::date
                        when academic_year = '2026-2027' then '2027-04-30'::date
                        else null
                    end
                )
            )
            else coalesce(slot_class_sec_end_date::date, current_date)
        end as effective_end
    from allocation_with_dow
),

planned_sessions_raw as (
    select
        *,
        case
            when slot_dow is not null and effective_start is not null and effective_end is not null
            then greatest(
                floor(
                    (
                        effective_end
                        - (effective_start + ((slot_dow - extract(dow from effective_start)::int + 7) % 7))
                    ) / 7.0
                )::int + 1,
                0
            )
            else 0
        end as planned_sessions_raw
    from planned_sessions_calc
),

planned_sessions as (
    select
        psr.*,
        greatest(psr.planned_sessions_raw - coalesce(c.total_cancellations, 0), 0) as planned_sessions
    from planned_sessions_raw psr
    left join {{ ref('fct_e2_cancellations') }} c
        on psr.slot_class_section_id = c.slot_class_section_id
        and psr.academic_year = c.academic_year
),

joined as (
    select
        ps.volunteer_id,
        ps.user_display_name as volunteer_name,
        ps.partner_id as chapter_id,
        ps.partner_name as chapter_name,
        ps.academic_year,
        ps.is_active,
        ps.planned_sessions,
        coalesce(aa.attended_sessions, 0) as attended_sessions,
        coalesce(aa.original_sessions, 0) as original_sessions,
        coalesce(aa.substitute_sessions, 0) as substitute_sessions
    from planned_sessions ps
    left join attendance_agg aa
        on ps.volunteer_id = aa.volunteer_id
        and ps.slot_class_section_id = aa.slot_class_section_id
        and ps.academic_year = aa.academic_year
),

volunteer_aggregated as (
    select
        volunteer_id,
        volunteer_name,
        chapter_id,
        chapter_name,
        academic_year,
        sum(planned_sessions) as planned_sessions,
        sum(attended_sessions) as attended_sessions,
        sum(original_sessions) as original_sessions,
        sum(substitute_sessions) as substitute_sessions,
        bool_or(is_active) as is_active
    from joined
    group by volunteer_id, volunteer_name, chapter_id, chapter_name, academic_year
)

select
    volunteer_id,
    volunteer_name,
    chapter_id,
    chapter_name,
    academic_year,
    is_active,
    planned_sessions,
    attended_sessions,
    original_sessions,
    substitute_sessions,
    -- TODO: update multiplier when actual session duration per slot is available.
    original_sessions * 2 as hours_contributed,
    case
        when planned_sessions = 0 then null
        else round(original_sessions::numeric / nullif(planned_sessions, 0) * 100, 1)
    end as attendance_pct,
    case
        when planned_sessions = 0 then 'No Sessions Yet'
        when round(original_sessions::numeric / nullif(planned_sessions, 0) * 100, 1) >= 90 then 'Healthy'
        when round(original_sessions::numeric / nullif(planned_sessions, 0) * 100, 1) >= 75 then 'At Risk'
        else 'Unhealthy'
    end as consistency_status
from volunteer_aggregated
order by chapter_id, consistency_status
