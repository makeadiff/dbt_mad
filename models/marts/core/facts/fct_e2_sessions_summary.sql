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
--
-- 2026-09-12 rework: total_planned_sessions is broken down using a historically-anchored, per-date
-- classification instead of "today's snapshot x whole window" (which silently rewrote past weeks
-- every time a volunteer got (re)assigned -- see fct_e2_planned_session_status for the full
-- rationale). Two populations feed the breakdown:
--   - Slotted sections (have >= 1 real scheduled slot_class_section row) -- authoritative source is
--     fct_e2_planned_session_status, one row per (real slot_class_section_id, planned_date), already
--     classified into Conducted / Cancelled / Volunteer Absent / No Volunteer Assigned / Not Yet Due
--     with a 7-day grace period and full multi-gap staffing history. This deliberately does NOT
--     assume "2 ideal slots" per class_section -- confirmed 2026-09-12 that 425 of 442 slotted
--     sections have only 1 real slot_class_section row (only 17 have 2). A section with 2 real slots
--     already gets 2x the rows here naturally (each real slot walks its own weekly date series), no
--     special-casing needed.
--   - Non-slotted sections (fct_e2_school_coverage.sections_without_volunteer -- despite its name,
--     this is "no slot scheduled at all", confirmed identical to class_sections_with_slot's own
--     population) -- no real schedule exists to project per-date status from at all.
--
-- 2026-09-16 rework (CHO-driven visibility): classes_without_assigned_volunteer and
-- classes_not_yet_due now count ONLY the slotted population -- an unslotted section contributes
-- ZERO to either, not the old "ideal 2 slots x elapsed/remaining weeks" estimate. The idea: a
-- section with no real slot yet isn't a "gap" to be estimated, it's simply not yet visible to
-- planning at all -- as and when a CHO actually creates a real slot for it, it starts counting (at
-- its own real slot count, not an assumed 2) and every downstream metric goes up accordingly.
-- classes_due_till_date (dashboard-facing alias: planned_session) is therefore now purely
-- slotted-based too: classes_conducted + classes_cancelled + classes_with_volunteer_absenteeism +
-- classes_without_assigned_volunteer -- no unslotted contribution anywhere in it.
-- total_planned_sessions (dashboard-facing alias: annual_planned_session) is the one exception that
-- KEEPS the unslotted contribution, at the ideal-2-slots rate, for the FULL window (not just elapsed)
-- -- it's the only remaining "aspirational full year" figure, by explicit request. Because of this,
-- total_planned_sessions is no longer exactly equal to the sum of the 5 breakdown columns: the
-- difference is precisely the unslotted contribution (unslotted_sections x 2 x weeks_in_window),
-- which is NOT separately broken out as its own column here (ideal_session_count, described below, is
-- a different, till-date-scoped figure, not this full-window gap).
-- ideal_session_count (new 2026-09-16) is a fully separate, standalone reference benchmark: total
-- sections (slotted + unslotted combined) x {{ var('ideal_slot_count') }} ideal slots x weeks_elapsed
-- (till today, NOT the full window) -- "what would be due by now if every section already had its
-- ideal number of slots." It doesn't feed into, and isn't reconciled against, any of the other
-- columns. The ideal slot count (currently {{ var('ideal_slot_count') }}) is a dbt var
-- (dbt_project.yml), not a hardcoded literal, along with the same constant used for the unslotted
-- contribution to total_planned_sessions above.
-- pct_sessions_happened compares classes_conducted against classes_due_till_date (now purely
-- slotted), not the full-year total_planned_sessions -- so a chapter partway through its year isn't
-- judged against a target it hasn't had time to reach yet.
-- total_original_sessions/total_substitute_sessions (the Conducted sub-split) are kept from the
-- original per-allocation attendance rollup, not re-derived from fct_e2_planned_session_status's
-- single 'Conducted' status -- they should tie closely to classes_conducted (both ultimately read the
-- same attendance records) but aren't forced to reconcile exactly, since they're built from a
-- slightly different population lens (allocation-history sections vs. slot-schedule sections).

with chapter_academic_years as (
    select
        sas.school_id::text as chapter_id,
        sas.partner_name as chapter_name,
        sas.academic_year,
        case when ccs.is_currently_active then 'Active' else 'Inactive' end as chapter_status
    from {{ ref('dim_school_academic_year_status') }} sas
    left join {{ ref('dim_chapter_current_status') }} ccs
        on sas.school_id = ccs.school_id
),

slotted_status_agg as (
    select
        school_id::text as chapter_id,
        academic_year,
        count(*) filter (where status = 'Conducted') as slotted_conducted,
        count(*) filter (where status = 'Cancelled') as slotted_cancelled,
        count(*) filter (where status = 'Volunteer Absent') as slotted_absent,
        count(*) filter (where status = 'No Volunteer Assigned') as slotted_no_volunteer,
        count(*) filter (where status = 'Not Yet Due') as slotted_not_yet_due
    from {{ ref('fct_e2_planned_session_status') }}
    group by school_id::text, academic_year
),

section_allocation as (
    -- Only used for the original/substitute sub-split of Conducted (see header) and the
    -- cancellation_reasons display text -- not for any of the core bucket counts anymore.
    select distinct on (slot_class_section_id, academic_year)
        partner_id,
        slot_class_section_id,
        academic_year
    from {{ ref('fct_e2_volunteer_allocation_history') }}
    order by slot_class_section_id, academic_year, volunteer_id
),

conducted_split_per_section as (
    select
        slot_class_section_id,
        academic_year,
        count(distinct date_of_slot) filter (where is_substitute = false) as original_sessions,
        count(distinct date_of_slot) filter (where is_substitute = true) as substitute_sessions
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    group by slot_class_section_id, academic_year
),

conducted_split_agg as (
    select
        sa.partner_id::text as chapter_id,
        sa.academic_year,
        sum(coalesce(cs.original_sessions, 0)) as total_original_sessions,
        sum(coalesce(cs.substitute_sessions, 0)) as total_substitute_sessions
    from section_allocation sa
    left join conducted_split_per_section cs
        on sa.slot_class_section_id = cs.slot_class_section_id
        and sa.academic_year = cs.academic_year
    group by sa.partner_id::text, sa.academic_year
),

cancellation_reasons_agg as (
    select
        sa.partner_id::text as chapter_id,
        sa.academic_year,
        string_agg(distinct c.cancellation_reasons, '; ' order by c.cancellation_reasons) as cancellation_reasons
    from section_allocation sa
    left join {{ ref('fct_e2_cancellations') }} c
        on sa.slot_class_section_id = c.slot_class_section_id
        and sa.academic_year = c.academic_year
    group by sa.partner_id::text, sa.academic_year
),

window_weeks as (
    select
        w.school_id::text as chapter_id,
        w.academic_year,
        coalesce(sc.total_sections, 0) as total_sections,
        coalesce(sc.sections_without_volunteer, 0) as unslotted_sections,
        case
            when w.window_start_date is not null and w.window_end_date is not null
            then greatest(floor((least(current_date, w.window_end_date) - w.window_start_date) / 7.0)::int, 0)
            else 0
        end as weeks_elapsed,
        case
            when w.window_start_date is not null and w.window_end_date is not null
            then greatest(floor((w.window_end_date - w.window_start_date) / 7.0)::int, 0)
            else 0
        end as weeks_in_window
    from {{ ref('dim_school_academic_year_window') }} w
    left join {{ ref('fct_e2_school_coverage') }} sc
        on w.school_id::text = sc.chapter_id
        and w.academic_year = sc.academic_year
),

bucketed as (
    select
        cay.chapter_id,
        cay.chapter_name,
        cay.chapter_status,
        cay.academic_year,
        coalesce(ssa.slotted_conducted, 0) as classes_conducted,
        coalesce(csa.total_original_sessions, 0) as total_original_sessions,
        coalesce(csa.total_substitute_sessions, 0) as total_substitute_sessions,
        coalesce(ssa.slotted_cancelled, 0) as classes_cancelled,
        coalesce(ssa.slotted_absent, 0) as classes_with_volunteer_absenteeism,
        coalesce(ssa.slotted_no_volunteer, 0) as classes_without_assigned_volunteer,
        coalesce(ssa.slotted_not_yet_due, 0) as classes_not_yet_due,
        coalesce(ww.unslotted_sections, 0) * {{ var('ideal_slot_count') }} * coalesce(ww.weeks_in_window, 0) as unslotted_ideal_contribution,
        coalesce(ww.total_sections, 0) * {{ var('ideal_slot_count') }} * coalesce(ww.weeks_elapsed, 0) as ideal_session_count,
        cra.cancellation_reasons
    from chapter_academic_years cay
    left join window_weeks ww
        on cay.chapter_id = ww.chapter_id
        and cay.academic_year = ww.academic_year
    left join slotted_status_agg ssa
        on cay.chapter_id = ssa.chapter_id
        and cay.academic_year = ssa.academic_year
    left join conducted_split_agg csa
        on cay.chapter_id = csa.chapter_id
        and cay.academic_year = csa.academic_year
    left join cancellation_reasons_agg cra
        on cay.chapter_id = cra.chapter_id
        and cay.academic_year = cra.academic_year
)

select
    b.chapter_id,
    b.chapter_name,
    cd.city_name,
    cd.state,
    cd.co_name,
    cd.engine,
    b.chapter_status,
    b.academic_year,
    b.classes_conducted
        + b.classes_cancelled
        + b.classes_with_volunteer_absenteeism
        + b.classes_without_assigned_volunteer
        + b.classes_not_yet_due
        + b.unslotted_ideal_contribution as total_planned_sessions,
    b.classes_conducted,
    b.total_original_sessions,
    b.total_substitute_sessions,
    b.classes_cancelled,
    b.classes_with_volunteer_absenteeism,
    b.classes_without_assigned_volunteer,
    b.classes_not_yet_due,
    b.ideal_session_count,
    (b.classes_conducted + b.classes_cancelled + b.classes_with_volunteer_absenteeism + b.classes_without_assigned_volunteer)
        as classes_due_till_date,
    round(
        b.classes_conducted::numeric
        / nullif(b.classes_conducted + b.classes_cancelled + b.classes_with_volunteer_absenteeism + b.classes_without_assigned_volunteer, 0)
        * 100,
        1
    ) as pct_sessions_happened,
    round(b.total_original_sessions::numeric / nullif(b.classes_conducted, 0) * 100, 1) as pct_original_sessions,
    round(b.total_substitute_sessions::numeric / nullif(b.classes_conducted, 0) * 100, 1) as pct_substitute_sessions,
    round(
        b.classes_cancelled::numeric
        / nullif(b.classes_conducted + b.classes_cancelled + b.classes_with_volunteer_absenteeism + b.classes_without_assigned_volunteer, 0)
        * 100,
        1
    ) as pct_cancellations,
    b.cancellation_reasons
from bucketed b
left join {{ ref('dim_chapter_mapping') }} cd
    on b.chapter_id = cd.chapter_id
