{{ config(materialized='table') }}

-- fct_e1_session_summary: school-level session delivery rollup for one academic year, for E1
-- (session-ops / platform_commons). E1 counterpart to fct_e2_sessions_summary.
-- Grain: one row per (school_id, academic_year)
-- Flow: int_pc_batch_coverage (batch -> school/academic_year, batch -> slot_shift) +
--       stg_pc_batch_attendance (session dates) + stg_pc_substitute (substitution/cancellation
--       events, keyed by slot_shift + date) + fct_e1_school_coverage (total_sections, for
--       total_planned_sessions) -> fct_e1_session_summary
-- The row set (which school+academic_year combos exist) is taken from fct_e1_school_coverage, so
-- both E1 facts share one consistent universe of school-years.
--
-- total_planned_sessions is a coarser estimate than E2's, by necessity: E2 computes it per
-- slot_class_section from that section's own day-of-week + an allocation start/end date.
-- platform_commons' worknode_slot_shift has no start-date field at all (nothing marks when a batch
-- was actually slotted into a shift), so that per-section calendar math isn't possible here. Using
-- platform_commons' own recorded slot_shift count instead (total_classes) was tried and rejected --
-- verified 2026-09-09 that total_classes/total_sections collapses from 2.02 (2023-2024) to 0.09
-- (2026-2027), i.e. most recent sections simply haven't been slotted into the system yet, so that
-- ratio reflects data-entry lag, not real scheduling. Instead: total_planned_sessions = total_sections
-- * {{ var('e1_planned_sessions_per_week', 2) }} (assumed sessions/week per section -- tunable via
-- the e1_planned_sessions_per_week var) * number of whole weeks between a fixed academic-year window
-- (July 1 of the AY's first year through March 30 of its second year, capped at today for the
-- current/ongoing year) -- e.g. AY "2026-2027" -> 2026-07-01 through min(today, 2027-03-30). This
-- applies uniformly to every section of a school+year regardless of when that section actually
-- started, which will overestimate total_planned_sessions (and therefore total_absenteeism) for
-- sections that started partway through the window -- a known tradeoff for not having a real
-- per-section start date to work with.

with batch_school_year as (
    select distinct school_id, academic_year, sc_level_batch_id
    from {{ ref('int_pc_batch_coverage') }}
),

batch_slot_shift as (
    select distinct sc_level_batch_id, worknode_slot_shift_id
    from {{ ref('int_pc_batch_coverage') }}
    where worknode_slot_shift_id is not null
),

-- Approved substitution/cancellation events, keyed by (slot_shift, date) -- the same grain as an
-- attendance record or a scheduled-but-uncaptured session. Only APPROVED events are counted --
-- confirmed a small number of SUBSTITUTE requests exist as REJECTED, which shouldn't count as an
-- actual substitution.
substitute_events as (
    select
        for_slot_shift_id,
        for_date::date as event_date,
        {{ clean_prefix('request_type') }} as request_type,
        requesting_reason
    from {{ ref('stg_pc_substitute') }}
    where request_status = 'SLOT_SHIFT_SUBSTITUTE_REQ_STATUS.APPROVED'
),

substitute_flag as (
    select distinct for_slot_shift_id, event_date
    from substitute_events
    where request_type = 'SUBSTITUTE'
),

-- Verified 2026-09-09: cancellation rows are already one row per (slot_shift, date) -- no fanout --
-- and only 369 of 29,973 overlap with an actual attendance record, consistent with "this scheduled
-- date was cancelled, no session happened" rather than a post-hoc annotation on a real session.
cancellations as (
    select
        bss.sc_level_batch_id,
        se.event_date,
        se.requesting_reason
    from substitute_events se
    join batch_slot_shift bss on se.for_slot_shift_id = bss.worknode_slot_shift_id
    where se.request_type = 'CANCELLATION'
),

attendance_dates as (
    select distinct
        ba.sc_level_batch_id,
        ba.attendance_date::date as session_date,
        ba.for_slot_shift_id
    from {{ ref('stg_pc_batch_attendance') }} ba
),

session_metrics_per_batch as (
    select
        ad.sc_level_batch_id,
        count(distinct ad.session_date) as sessions_happened,
        count(distinct ad.session_date) filter (where sf.for_slot_shift_id is null) as original_sessions,
        count(distinct ad.session_date) filter (where sf.for_slot_shift_id is not null) as substitute_sessions
    from attendance_dates ad
    left join substitute_flag sf
        on ad.for_slot_shift_id = sf.for_slot_shift_id
        and ad.session_date = sf.event_date
    group by ad.sc_level_batch_id
),

cancellations_per_batch as (
    select
        sc_level_batch_id,
        count(*) as total_cancellations,
        string_agg(distinct requesting_reason, '; ' order by requesting_reason) as cancellation_reasons
    from cancellations
    group by sc_level_batch_id
),

batch_metrics as (
    select
        bsy.school_id,
        bsy.academic_year,
        coalesce(sum(smb.sessions_happened), 0) as total_sessions_happened,
        coalesce(sum(smb.original_sessions), 0) as total_original_sessions,
        coalesce(sum(smb.substitute_sessions), 0) as total_substitute_sessions,
        coalesce(sum(cpb.total_cancellations), 0) as total_cancellations,
        string_agg(distinct cpb.cancellation_reasons, '; ' order by cpb.cancellation_reasons) as cancellation_reasons
    from batch_school_year bsy
    left join session_metrics_per_batch smb on bsy.sc_level_batch_id = smb.sc_level_batch_id
    left join cancellations_per_batch cpb on bsy.sc_level_batch_id = cpb.sc_level_batch_id
    group by bsy.school_id, bsy.academic_year
),

-- Fixed academic-year window: July 1 of the AY's first year through March 30 of its second year,
-- capped at today so an in-progress year doesn't count future weeks as "planned." Malformed/blank
-- academic_year values (a handful of batches have no academicYear at source) fall through to null
-- window bounds and therefore a null total_planned_sessions, rather than erroring.
planned_sessions_window as (
    select distinct
        academic_year,
        case when academic_year ~ '^[0-9]{4}-[0-9]{4}$'
            then make_date(split_part(academic_year, '-', 1)::int, 7, 1)
        end as window_start,
        case when academic_year ~ '^[0-9]{4}-[0-9]{4}$'
            then least(current_date, make_date(split_part(academic_year, '-', 2)::int, 3, 30))
        end as window_end
    from {{ ref('fct_e1_school_coverage') }}
),

planned_sessions as (
    select
        academic_year,
        greatest(floor((window_end - window_start) / 7.0)::int, 0) as weeks_in_window
    from planned_sessions_window
)

select
    sc.school_id,
    sc.academic_year,
    sc.total_sections * {{ var('e1_planned_sessions_per_week', 2) }} * ps.weeks_in_window as total_planned_sessions,
    bm.total_sessions_happened,
    bm.total_original_sessions,
    bm.total_substitute_sessions,
    round(bm.total_original_sessions * 100.0 / nullif(bm.total_sessions_happened, 0), 1) as pct_original_sessions,
    round(bm.total_substitute_sessions * 100.0 / nullif(bm.total_sessions_happened, 0), 1) as pct_substitute_sessions,
    bm.total_cancellations,
    bm.cancellation_reasons,
    greatest(
        (sc.total_sections * {{ var('e1_planned_sessions_per_week', 2) }} * ps.weeks_in_window)
            - bm.total_sessions_happened - bm.total_cancellations,
        0
    ) as total_absenteeism,
    round(
        bm.total_sessions_happened * 100.0
            / nullif(sc.total_sections * {{ var('e1_planned_sessions_per_week', 2) }} * ps.weeks_in_window, 0),
        1
    ) as pct_sessions_happened,
    round(
        bm.total_cancellations * 100.0
            / nullif(sc.total_sections * {{ var('e1_planned_sessions_per_week', 2) }} * ps.weeks_in_window, 0),
        1
    ) as pct_cancellations
from {{ ref('fct_e1_school_coverage') }} sc
left join batch_metrics bm
    on sc.school_id = bm.school_id
    and sc.academic_year = bm.academic_year
left join planned_sessions ps
    on sc.academic_year = ps.academic_year
