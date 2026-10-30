{{ config(materialized='table') }}

-- CCI dashboard (E1): sessions cancelled on a mentoring circle from 1 October 2026 (spec §3.10),
-- one row per circle per date. A cancellation is an approved, active CANCELLATION request in
-- stg_pc_substitute on the circle's slot shift, with nothing held that date. Not read by Dalgo.
--
-- Reason: the requesting reason. Readable ones ("MAD Event", "not enough volunteers") show as
-- logged; the coded ones (CLASS_CANCELLATION_REASON.REASON_1 to REASON_6) show as "Reason code 1"
-- etc. until the mapping is given; blank is "Reason not logged".
-- Today (30 Oct 2026) the table has no cancellation request dated after April 2026, so this
-- model is empty until Platform Commons logs one.
--
-- Grain: one row per (worknode_slot_shift_id, session_date).

with requests as (
    select
        s.for_slot_shift_id as worknode_slot_shift_id,
        coalesce(s.original_slot_date, s.for_date)::date as session_date,
        min(
            case
                when nullif(trim(s.requesting_reason), '') is null then 'Reason not logged'
                when s.requesting_reason ~ 'REASON_[0-9]+$'
                    then 'Reason code ' || substring(s.requesting_reason from 'REASON_([0-9]+)$')
                else trim(s.requesting_reason)
            end
        ) as cancel_reason
    from {{ ref('stg_pc_substitute') }} s
    where s.request_type like '%CANCELLATION'
      and s.request_status like '%APPROVED'
      and s.is_active
    group by 1, 2
)

select
    c.chapter_id,
    r.worknode_slot_shift_id,
    r.session_date,
    date_trunc('week', r.session_date)::date as week_start,
    r.cancel_reason
from requests r
join {{ ref('prod_cci_mentoring_circles') }} c
    on r.worknode_slot_shift_id = c.worknode_slot_shift_id
left join {{ ref('prod_cci_circle_sessions') }} h
    on r.worknode_slot_shift_id = h.worknode_slot_shift_id
   and r.session_date = h.session_date
where r.session_date >= date '2026-10-01'
  and h.worknode_slot_shift_id is null
