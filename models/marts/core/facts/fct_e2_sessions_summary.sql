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
-- total_planned_sessions is NOT derived from volunteer allocation (i.e. not "planned once a volunteer
-- got assigned into a slot") -- it's total_sections (from fct_e2_school_coverage) x 2 ideal weekly
-- slots x the number of weeks in this chapter+AY's planned window (from
-- dim_school_academic_year_window, which resolves start/end dates from school_session_detail or,
-- almost always in practice, a matching MOU). Weeks-in-window is floor((end - start) / 7) -- a
-- partial trailing week (e.g. 37 weeks + 1 leftover day) is dropped, not rounded up to a full week.
-- This is a deliberate scope split: only this dashboard's total_planned_sessions changes --
-- fct_e2_volunteer_consistency / fct_e2_child_consistency keep the original per-allocation
-- planned_sessions logic untouched, since they need a per-volunteer number that this chapter-level
-- target can't provide.
-- total_planned_sessions is broken down into four mutually-exclusive buckets, plus a 5th residual:
--   classes_conducted -- total_sessions_happened (below), a class actually ran (original volunteer
--     or a substitute).
--   total_cancellations -- already existed; sessions cancelled by a school_holiday (fct_e2_cancellations).
--   classes_with_volunteer_absenteeism (2026-09-11) -- a DOTS attendance record exists with
--     attendance='FALSE' and no substitute_volunteer_id logged: the volunteer was marked absent and
--     nobody covered. Confirmed this is genuinely rare today (10 of 584 attendance rows warehouse-wide).
--     This also fixes a latent bug: `is_substitute` (on fct_e2_volunteer_attendance_by_slot_date)
--     evaluates to NULL, not false, for exactly these rows (`attendance='FALSE' AND <null-substitute>`
--     is NULL under three-valued logic, not false) -- so they were silently counted in the old
--     sessions_happened (which had no is_substitute filter at all) while landing in neither
--     original_sessions nor substitute_sessions. sessions_happened now excludes them explicitly.
--   classes_without_assigned_volunteer (2026-09-11) -- sections with no volunteer ever assigned at
--     all (fct_e2_school_coverage.sections_without_assigned_volunteer x 2 x weeks_in_window) -- these
--     sections don't appear in fct_e2_volunteer_allocation_history at all, so they contribute 0 to
--     every other bucket by construction.
--   classes_unexplained_other -- total_planned_sessions minus the four buckets above. Not forced to
--     zero: the four buckets are built from two different section populations (allocation-history-based
--     for conducted/cancelled/absenteeism vs. the full class_section count from fct_e2_school_coverage
--     for total_planned_sessions/without_assigned_volunteer), so a real, visible gap can remain --
--     surfaced here rather than silently absorbed into one of the other four.

with section_allocation as (
    -- Only used to map each slot_class_section_id to its chapter (partner_id), so
    -- sessions_happened/cancellations below can be rolled up to chapter+academic_year grain.
    select distinct on (slot_class_section_id, academic_year)
        partner_id,
        slot_class_section_id,
        academic_year
    from {{ ref('fct_e2_volunteer_allocation_history') }}
    order by slot_class_section_id, academic_year, volunteer_id
),

planned_sessions_window as (
    select
        w.school_id::text as chapter_id,
        w.academic_year,
        case
            when w.window_start_date is not null and w.window_end_date is not null
            then greatest(floor((w.window_end_date - w.window_start_date) / 7.0)::int, 0)
            else 0
        end as weeks_in_window,
        coalesce(sc.total_sections, 0) * 2 * case
            when w.window_start_date is not null and w.window_end_date is not null
            then greatest(floor((w.window_end_date - w.window_start_date) / 7.0)::int, 0)
            else 0
        end as total_planned_sessions,
        coalesce(sc.sections_without_assigned_volunteer, 0) * 2 * case
            when w.window_start_date is not null and w.window_end_date is not null
            then greatest(floor((w.window_end_date - w.window_start_date) / 7.0)::int, 0)
            else 0
        end as classes_without_assigned_volunteer
    from {{ ref('dim_school_academic_year_window') }} w
    left join {{ ref('fct_e2_school_coverage') }} sc
        on w.school_id::text = sc.chapter_id
        and w.academic_year = sc.academic_year
),

sessions_happened_per_section as (
    select
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) filter (where is_substitute is not null) as sessions_happened,
        count(distinct date_of_slot) filter (where is_substitute = false) as original_sessions,
        count(distinct date_of_slot) filter (where is_substitute = true) as substitute_sessions,
        count(distinct date_of_slot) filter (where is_substitute is null) as absenteeism_sessions
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
        sa.partner_id,
        sa.slot_class_section_id,
        sa.academic_year,
        coalesce(h.sessions_happened, 0) as sessions_happened,
        coalesce(h.original_sessions, 0) as original_sessions,
        coalesce(h.substitute_sessions, 0) as substitute_sessions,
        coalesce(h.absenteeism_sessions, 0) as absenteeism_sessions,
        coalesce(c.total_cancellations, 0) as total_cancellations,
        c.cancellation_reasons
    from section_allocation sa
    left join sessions_happened_per_section h
        on sa.slot_class_section_id = h.slot_class_section_id
        and sa.academic_year = h.academic_year
    left join {{ ref('fct_e2_cancellations') }} c
        on sa.slot_class_section_id = c.slot_class_section_id
        and sa.academic_year = c.academic_year
),

section_metrics_agg as (
    select
        sm.partner_id::text as partner_id,
        sm.academic_year,
        sum(sm.sessions_happened) as total_sessions_happened,
        sum(sm.original_sessions) as total_original_sessions,
        sum(sm.substitute_sessions) as total_substitute_sessions,
        sum(sm.absenteeism_sessions) as classes_with_volunteer_absenteeism,
        sum(sm.total_cancellations) as total_cancellations,
        string_agg(distinct sm.cancellation_reasons, '; ' order by sm.cancellation_reasons) as cancellation_reasons
    from section_metrics sm
    group by sm.partner_id::text, sm.academic_year
)

select
    cay.chapter_id,
    cay.chapter_name,
    cd.city_name,
    cd.state,
    cd.co_name,
    cd.engine,
    cay.chapter_status,
    cay.academic_year,
    coalesce(psw.total_planned_sessions, 0) as total_planned_sessions,
    coalesce(sma.total_sessions_happened, 0) as total_sessions_happened,
    coalesce(sma.total_original_sessions, 0) as total_original_sessions,
    coalesce(sma.total_substitute_sessions, 0) as total_substitute_sessions,
    coalesce(sma.total_cancellations, 0) as total_cancellations,
    coalesce(sma.classes_with_volunteer_absenteeism, 0) as classes_with_volunteer_absenteeism,
    coalesce(psw.classes_without_assigned_volunteer, 0) as classes_without_assigned_volunteer,
    coalesce(psw.total_planned_sessions, 0)
        - coalesce(sma.total_sessions_happened, 0)
        - coalesce(sma.total_cancellations, 0)
        - coalesce(sma.classes_with_volunteer_absenteeism, 0)
        - coalesce(psw.classes_without_assigned_volunteer, 0) as classes_unexplained_other,
    round(coalesce(sma.total_sessions_happened, 0)::numeric / nullif(psw.total_planned_sessions, 0) * 100, 1) as pct_sessions_happened,
    round(coalesce(sma.total_original_sessions, 0)::numeric / nullif(sma.total_sessions_happened, 0) * 100, 1) as pct_original_sessions,
    round(coalesce(sma.total_substitute_sessions, 0)::numeric / nullif(sma.total_sessions_happened, 0) * 100, 1) as pct_substitute_sessions,
    round(coalesce(sma.total_cancellations, 0)::numeric / nullif(psw.total_planned_sessions, 0) * 100, 1) as pct_cancellations,
    sma.cancellation_reasons
from chapter_academic_years cay
left join {{ ref('dim_chapter_mapping') }} cd
    on cay.chapter_id = cd.chapter_id
left join planned_sessions_window psw
    on cay.chapter_id = psw.chapter_id
    and cay.academic_year = psw.academic_year
left join section_metrics_agg sma
    on cay.chapter_id = sma.partner_id
    and cay.academic_year = sma.academic_year
