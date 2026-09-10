{{ config(materialized='table') }}

-- fct_e1_cancellation_reasons: cancelled sessions broken down by cancellation reason, per school
-- per academic year, for E1 (session-ops / platform_commons).
-- Grain: one row per (school_id, academic_year, cancellation_reason)
-- E1 counterpart to fct_e2_cancellation_reasons, but built much more directly: E2 has to project
-- planned session dates from an allocation start/end date and then match those dates against a
-- separate school_holiday calendar, because Bubble has no first-class "this session was cancelled"
-- record. Platform_commons does -- stg_pc_substitute already carries one row per (slot_shift, date)
-- cancellation event with its own reason text, so this is a straight group-by, no date projection
-- needed. Kept as a sibling of fct_e1_session_summary rather than built on top of it, since
-- fct_e1_session_summary only exposes a semicolon-joined distinct-reasons string per school/year,
-- not per-reason counts -- same relationship fct_e2_cancellation_reasons has to fct_e2_cancellations.
-- cancellation_reason is platform_commons' raw requesting_reason text (e.g. "MAD Event",
-- "not enough volunteers", "REASON_1"..."REASON_6" -- opaque coded reasons not yet mapped to labels
-- in this warehouse) -- passed through as-is rather than remapped, so new values show up
-- automatically.

with batch_slot_shift as (
    select distinct sc_level_batch_id, worknode_slot_shift_id
    from {{ ref('int_pc_batch_coverage') }}
    where worknode_slot_shift_id is not null
),

batch_school_year as (
    select distinct school_id, academic_year, sc_level_batch_id
    from {{ ref('int_pc_batch_coverage') }}
),

-- Only APPROVED cancellation requests count -- same filter fct_e1_session_summary applies.
cancellation_events as (
    select
        for_slot_shift_id,
        requesting_reason
    from {{ ref('stg_pc_substitute') }}
    where request_status = 'SLOT_SHIFT_SUBSTITUTE_REQ_STATUS.APPROVED'
      and request_type = 'SLOT_SHIFT_SUBSTITUTE_REQ_TYPE.CANCELLATION'
)

select
    bsy.school_id,
    bsy.academic_year,
    ce.requesting_reason as cancellation_reason,
    count(*) as cancelled_sessions_count
from cancellation_events ce
join batch_slot_shift bss on ce.for_slot_shift_id = bss.worknode_slot_shift_id
join batch_school_year bsy on bss.sc_level_batch_id = bsy.sc_level_batch_id
group by bsy.school_id, bsy.academic_year, ce.requesting_reason
