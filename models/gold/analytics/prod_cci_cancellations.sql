{{ config(materialized='table') }}

-- CCI dashboard (E1): sessions cancelled on a mentoring circle from 1 October 2026 (spec §3.10),
-- one row per circle per date. Since layout v4 (spec §7 L1) these are the planned circle-dates in
-- the cancelled group of prod_cci_planned_sessions (an approved, active CANCELLATION request in
-- stg_pc_substitute on the slot date or the day before, nothing held), so the reasons add up to the
-- breakdown's Cancelled. Not read by Dalgo.
--
-- Reason: the requesting reason. Readable ones ("MAD Event", "not enough volunteers") show as
-- logged; the coded ones (CLASS_CANCELLATION_REASON.REASON_1 to REASON_6) show as "Reason code 1"
-- etc. until the mapping is given; blank is "Reason not logged". Today (12 Nov 2026) stg_pc_substitute
-- has no request dated after 25 April 2026, so this model is empty until Platform Commons logs one.
--
-- Grain: one row per (worknode_slot_shift_id, session_date).

select
    chapter_id,
    worknode_slot_shift_id,
    session_date,
    week_start,
    cancel_reason
from {{ ref('prod_cci_planned_sessions') }}
where session_group = 'cancelled'
