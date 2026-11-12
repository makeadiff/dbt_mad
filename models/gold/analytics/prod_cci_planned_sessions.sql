{{ config(materialized='table') }}

-- CCI dashboard (E1): every planned circle-date from 2026-10-01, cancelled ones kept in, each in
-- exactly one group (spec E1 §7 L1, copying DO_DASHBOARD_SPEC.md §3.8 rulings 1 and 2 and
-- prod_do_circle_sessions). Base for prod_cci_weekly_sessions, prod_cci_session_breakdown,
-- prod_cci_cancellations and the chapter summary's planned / held counts, so the planned, held and
-- group rules live in one place. Not read by Dalgo. prod_cci_circle_sessions (held sessions since
-- 1 June, any date) stays the base for the volunteer and child consistency tabs.
--
-- Planned: every circle with a weekly slot day (prod_cci_mentoring_circles.slot_dow), one session a
-- week on that day from 2026-10-01 to today, whether or not it has a volunteer. Cancelled dates stay
-- in, so planned is one figure everywhere.
-- Groups, first that applies:
--   cancelled             an approved, active CANCELLATION request (stg_pc_substitute), nothing held
--   held_by_original      a volunteer marked present
--   held_by_substitute    no volunteer present, a substitute present
--   volunteer_absent      nothing held, and an attendance record exists or the circle has a volunteer
--   no_volunteer_assigned nothing held, nothing logged and no volunteer on the circle
-- Volunteer on the circle: Platform Commons' circle user list has no start or end date, so this is
-- the circle's current list (prod_cci_mentoring_circles.volunteers > 0), not a per-date allocation
-- as School reconstructs from Bubble.
--
-- Attendance date: int_pc_volunteer_attendance logs volunteer sessions one day before the slot day
-- (spec §6 known defect; checked 2026-11-12, 153 of 153 held sessions since 1 Oct are slot day - 1).
-- So a planned date matches attendance or a cancellation on that date or the day before. Weeks are
-- the planned (slot) date's week, which also stops Sunday sessions slipping into the week before.
--
-- Cancel reason: the requesting reason; coded ones (REASON_1..6) as "Reason code N", blank as
-- "Reason not logged" (spec §3.10). No 2026-27 request exists yet (last one dated 25 Apr 2026).
--
-- Grain: one row per (worknode_slot_shift_id, session_date).

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

circles as (
    select chapter_id, worknode_slot_shift_id, slot_dow, volunteers
    from {{ ref('prod_cci_mentoring_circles') }}
    where slot_dow is not null
),

circle_dates as (
    select c.chapter_id, c.worknode_slot_shift_id, c.volunteers, d::date as session_date
    from circles c
    cross join params p
    cross join lateral generate_series(
        p.tracking_start + ((c.slot_dow - extract(dow from p.tracking_start)::int + 7) % 7),
        p.today,
        interval '7 days'
    ) as d
),

attendance as (
    select
        a.section_slot_shift_id as worknode_slot_shift_id,
        a.scheduled_session_date::date as attendance_date,
        bool_or(a.attendance_status = 'PRESENT') as has_present,
        bool_or(a.substitute_volunteer_attendance_status = 'PRESENT') as has_substitute
    from {{ ref('int_pc_volunteer_attendance') }} a
    where a.scheduled_session_date::date >= date '2026-09-30'
    group by 1, 2
),

cancellations as (
    select
        s.for_slot_shift_id as worknode_slot_shift_id,
        coalesce(s.original_slot_date, s.for_date)::date as cancel_date,
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
),

flagged as (
    select
        cd.chapter_id,
        cd.worknode_slot_shift_id,
        cd.session_date,
        cd.volunteers > 0 as has_volunteer,
        coalesce(bool_or(at.has_present), false) as has_present,
        coalesce(bool_or(at.has_substitute), false) as has_substitute,
        count(at.worknode_slot_shift_id) > 0 as has_attendance_record
    from circle_dates cd
    left join attendance at
        on cd.worknode_slot_shift_id = at.worknode_slot_shift_id
       and at.attendance_date in (cd.session_date, cd.session_date - 1)
    group by 1, 2, 3, 4
),

cancelled as (
    select cd.worknode_slot_shift_id, cd.session_date, min(cx.cancel_reason) as cancel_reason
    from circle_dates cd
    join cancellations cx
        on cd.worknode_slot_shift_id = cx.worknode_slot_shift_id
       and cx.cancel_date in (cd.session_date, cd.session_date - 1)
    group by 1, 2
)

select
    f.chapter_id,
    f.worknode_slot_shift_id,
    f.session_date,
    date_trunc('week', f.session_date)::date as week_start,
    case
        when cx.worknode_slot_shift_id is not null and not (f.has_present or f.has_substitute) then 'cancelled'
        when f.has_present then 'held_by_original'
        when f.has_substitute then 'held_by_substitute'
        when f.has_attendance_record or f.has_volunteer then 'volunteer_absent'
        else 'no_volunteer_assigned'
    end as session_group,
    f.has_volunteer,
    case
        when cx.worknode_slot_shift_id is not null and not (f.has_present or f.has_substitute)
        then cx.cancel_reason
    end as cancel_reason
from flagged f
left join cancelled cx
    on f.worknode_slot_shift_id = cx.worknode_slot_shift_id
   and f.session_date = cx.session_date
