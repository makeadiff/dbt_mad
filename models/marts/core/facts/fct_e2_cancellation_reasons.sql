{{ config(materialized='table') }}

-- fct_e2_cancellation_reasons: cancelled sessions broken down by cancellation reason, per chapter per academic year
-- Grain: one row per (chapter_id, academic_year, cancellation_reason)
-- 2026-09-12 rework: built directly on fct_e2_planned_session_status's 'Cancelled' rows, so counts
-- reconcile exactly with fct_e2_sessions_summary.classes_cancelled -- the old version projected its
-- own separate weekly date-walk off fct_e2_volunteer_allocation_history (the pre-rework population),
-- which no longer matches the new classes_cancelled total (confirmed: chapter 592 AY2026-2027 showed
-- classes_cancelled=36, but the old logic here produced an unreconciled count against a different
-- section population).
-- cancellation_reason is holiday_reason (per explicit 2026-09-12 request) -- note this only ever
-- holds one of 2 coarse values warehouse-wide ("Holidays" / "Cancelled from school's end"), unlike
-- holiday_description which carries the actual specific reason (e.g. "Onam", "Teachers Day"). If a
-- finer-grained breakdown is wanted later, holiday_description is the field to switch to.
-- A cancelled (slot_class_section_id, planned_date) row can technically overlap more than one
-- school_holiday window for the same school -- distinct on (slot_class_section_id, planned_date)
-- picks one reason per row (same tie-break style fct_e2_cancellations uses), so counts still sum to
-- classes_cancelled exactly, never double-counting a single cancelled session across reasons.

with cancelled_sessions as (
    select
        school_id,
        academic_year,
        slot_class_section_id,
        planned_date
    from {{ ref('fct_e2_planned_session_status') }}
    where status = 'Cancelled'
),

cancelled_with_reason as (
    select distinct on (cs.slot_class_section_id, cs.planned_date)
        cs.school_id::text as chapter_id,
        cs.academic_year,
        coalesce(nullif(trim(sh.holiday_reason), ''), 'Unspecified') as cancellation_reason
    from cancelled_sessions cs
    join {{ ref('int_bubble__school_holiday') }} sh
        on cs.school_id = sh.school_id
        and cs.planned_date >= sh.start_date
        and cs.planned_date <= sh.end_date
        and sh.is_removed = false
    order by cs.slot_class_section_id, cs.planned_date, cancellation_reason
)

select
    chapter_id,
    academic_year,
    cancellation_reason,
    count(*) as cancelled_sessions_count
from cancelled_with_reason
group by chapter_id, academic_year, cancellation_reason
