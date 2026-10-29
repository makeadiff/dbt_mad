{{ config(materialized='table') }}

-- CCI dashboard (E1): the dates each mentoring circle held a session since 1 June 2026 (start of the
-- 2026-27 sections). Shared by prod_cci_mentoring_circles (started, sessions since 1 Oct) and
-- prod_cci_weekly_sessions, so "session held" is defined once. Not read by Dalgo.
--
-- Session held (spec E1 §3.4): a volunteer attendance record on the circle's slot shift marked
-- PRESENT, or a substitute marked present (none logged this year), from int_pc_volunteer_attendance.
-- Counted once per circle per date. Whether the shift is a live circle is decided in
-- prod_cci_mentoring_circles; this model keeps every shift's dates and is filtered there.
--
-- Grain: one row per (worknode_slot_shift_id, session_date).

select distinct
    section_slot_shift_id as worknode_slot_shift_id,
    scheduled_session_date::date as session_date
from {{ ref('int_pc_volunteer_attendance') }}
where (attendance_status = 'PRESENT' or substitute_volunteer_attendance_status = 'PRESENT')
  and scheduled_session_date::date >= date '2026-06-01'
