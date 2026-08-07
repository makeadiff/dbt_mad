{{ config(materialized='table') }}

-- fct_e2_sessions_summary: chapter-level session delivery rollup for one academic year
-- Grain: one row per (chapter_id, academic_year)
-- Ported from the legacy fct_e2_sessions_summary model, rebuilt on fct_e2_volunteer_allocation_history +
-- fct_e2_volunteer_attendance_by_slot_date + fct_e2_cancellations.
-- The row set (which chapter+academic_year combos exist at all) comes from
-- dim_school_academic_year_status, not dim_chapter_mapping's Active/E2 filter -- that filter reflects
-- today's ops-sheet status, which excludes schools that have real historical data but are since marked
-- "Dropped out" or were never added to the sheet (confirmed: 35 + 15 such schools for 2025-2026 alone).
-- dim_chapter_mapping is still joined, but only to enrich with fields Bubble doesn't have (city/CO/CHO)
-- -- it no longer decides which rows appear.
-- No explicit engine filter: every school in Bubble's school_academic_year table is assumed to be E2
-- (Bubble/DOTS is the E2-specific tracking system; E1 has no presence here at all). If that assumption
-- ever stops holding, this will need a real engine filter added back.
-- chapter_status comes from dim_chapter_current_status ('Active'/'Inactive'), not from a given year's
-- own is_ay_active -- a chapter's OLD year showing inactive is usually just normal rollover once a
-- newer year exists (confirmed: of 101 chapters inactive for 2025-2026, 62 are active again in
-- 2026-2027), so per-year active/inactive can't honestly be read as "dropped out." This instead
-- reports whether the chapter's single latest known academic-year record is active, repeated across
-- every row for that chapter regardless of which year the row itself is for.

with section_allocation as (
    select distinct on (slot_class_section_id, academic_year)
        partner_id,
        slot_class_section_id,
        academic_year,
        day_of_week,
        sessions_start_date,
        slot_class_sec_end_date
    from {{ ref('fct_e2_volunteer_allocation_history') }}
    order by slot_class_section_id, academic_year, volunteer_id
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
    from section_allocation
),

planned_sessions_calc as (
    select
        *,
        sessions_start_date::date as effective_start,
        least(
            current_date,
            coalesce(
                slot_class_sec_end_date::date,
                case
                    when academic_year = '2025-2026' then '2026-04-30'::date
                    when academic_year = '2026-2027' then '2027-04-30'::date
                    else null
                end
            )
        ) as effective_end
    from allocation_with_dow
),

planned_sessions_per_section as (
    select
        partner_id,
        slot_class_section_id,
        academic_year,
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
        end as planned_sessions
    from planned_sessions_calc
),

sessions_happened_per_section as (
    select
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) as sessions_happened,
        count(distinct date_of_slot) filter (where is_substitute = false) as original_sessions,
        count(distinct date_of_slot) filter (where is_substitute = true) as substitute_sessions
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    group by slot_class_section_id, academic_year
),

chapter_academic_years as (
    select
        sas.school_id::text as chapter_id,
        sas.partner_name as chapter_name,
        sas.academic_year,
        case when ccs.is_currently_active then 'Active' else 'Inactive' end as chapter_status
    from {{ ref('dim_school_academic_year_status') }} sas
    left join {{ ref('dim_chapter_current_status') }} ccs
        on sas.school_id = ccs.school_id
),

section_metrics as (
    select
        p.partner_id,
        p.slot_class_section_id,
        p.academic_year,
        p.planned_sessions,
        coalesce(h.sessions_happened, 0) as sessions_happened,
        coalesce(h.original_sessions, 0) as original_sessions,
        coalesce(h.substitute_sessions, 0) as substitute_sessions,
        coalesce(c.total_cancellations, 0) as total_cancellations,
        c.cancellation_reasons
    from planned_sessions_per_section p
    left join sessions_happened_per_section h
        on p.slot_class_section_id = h.slot_class_section_id
        and p.academic_year = h.academic_year
    left join {{ ref('fct_e2_cancellations') }} c
        on p.slot_class_section_id = c.slot_class_section_id
        and p.academic_year = c.academic_year
),

section_metrics_agg as (
    select
        sm.partner_id::text as partner_id,
        sm.academic_year,
        sum(sm.planned_sessions) as total_planned_sessions,
        sum(sm.sessions_happened) as total_sessions_happened,
        sum(sm.original_sessions) as total_original_sessions,
        sum(sm.substitute_sessions) as total_substitute_sessions,
        greatest(
            sum(sm.planned_sessions) - sum(sm.sessions_happened) - sum(sm.total_cancellations),
            0
        ) as total_absenteeism,
        sum(sm.total_cancellations) as total_cancellations,
        round(sum(sm.sessions_happened)::numeric / nullif(sum(sm.planned_sessions), 0) * 100, 1) as pct_sessions_happened,
        round(sum(sm.original_sessions)::numeric / nullif(sum(sm.sessions_happened), 0) * 100, 1) as pct_original_sessions,
        round(sum(sm.substitute_sessions)::numeric / nullif(sum(sm.sessions_happened), 0) * 100, 1) as pct_substitute_sessions,
        round(sum(sm.total_cancellations)::numeric / nullif(sum(sm.planned_sessions), 0) * 100, 1) as pct_cancellations,
        string_agg(distinct sm.cancellation_reasons, '; ' order by sm.cancellation_reasons) as cancellation_reasons
    from section_metrics sm
    group by sm.partner_id::text, sm.academic_year
)

select
    cay.chapter_id,
    cay.chapter_name,
    cd.city_name,
    cd.co_name,
    cd.engine,
    cay.chapter_status,
    cay.academic_year,
    sma.total_planned_sessions,
    sma.total_sessions_happened,
    sma.total_original_sessions,
    sma.total_substitute_sessions,
    sma.total_absenteeism,
    sma.total_cancellations,
    sma.pct_sessions_happened,
    sma.pct_original_sessions,
    sma.pct_substitute_sessions,
    sma.pct_cancellations,
    sma.cancellation_reasons
from chapter_academic_years cay
left join {{ ref('dim_chapter_mapping') }} cd
    on cay.chapter_id = cd.chapter_id
left join section_metrics_agg sma
    on cay.chapter_id = sma.partner_id
    and cay.academic_year = sma.academic_year
