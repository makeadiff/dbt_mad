{{ config(materialized='table') }}

-- DO dashboard (E2): every planned circle-date from 2026-10-01, cancelled ones kept in, each in
-- exactly one group (spec §3.7 R2). Base for prod_do_session_breakdown, prod_do_cancellation_reasons
-- and prod_do_child_sessions. Not read by Dalgo.
--
-- Same dates as prod_do_weekly_sessions (§3.3b): every circle with a slot (prod_do_mentoring_circles),
-- one session a week on its slot day, from the later of 2026-10-01 and the slot-link start, to today.
-- Unlike the weekly model, cancelled dates are kept. Groups, first that applies:
--   cancelled          a cancellation logged (fct_e2_planned_session_status 'Cancelled'), nothing held
--   held_by_original   a volunteer present
--   held_by_substitute no volunteer present, a substitute logged
--   volunteer_absent   attendance records, none present, no substitute
--   no_attendance      no attendance record at all
-- Cancelled dates with something held count as held, as in the weekly model, so planned minus
-- cancelled equals the weekly "Sessions planned".
--
-- Cancel reason: the holiday reason logged in Bubble (int_bubble__school_holiday) for the school
-- on that date, as is; the first when several overlap.
-- Hours: the slot's start to end time (int_bubble__slot); 1 hour when a time is missing or the end
-- isn't after the start (slot_times_missing says which).
--
-- Grain: one row per (slot_id, class_section_id, session_date).

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

circles as (
    select chapter_id, slot_id, class_section_id, slot_dow, slot_link_start_date
    from {{ ref('prod_do_mentoring_circles') }}
    where slot_dow is not null
),

circle_dates as (
    select c.chapter_id, c.slot_id, c.class_section_id, d::date as session_date, c.slot_link_start_date
    from circles c
    cross join params p
    cross join lateral generate_series(
        greatest(p.tracking_start, c.slot_link_start_date)
            + ((c.slot_dow - extract(dow from greatest(p.tracking_start, c.slot_link_start_date))::int + 7) % 7),
        p.today,
        interval '7 days'
    ) as d
),

attendance as (
    select
        scs.slot_id,
        scs.class_section_id,
        a.date_of_slot::date as session_date,
        bool_or(upper(trim(a.attendance)) = 'TRUE') as has_present,
        bool_or(a.is_substitute) as has_substitute
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }} a
    join {{ ref('int_bubble__slot_class_section') }} scs
        on a.slot_class_section_id = scs.slot_class_section_id
    where a.academic_year = '2026-2027'
    group by 1, 2, 3
),

cancelled_dates as (
    select distinct scs.slot_id, scs.class_section_id, p.planned_date as session_date
    from {{ ref('fct_e2_planned_session_status') }} p
    join {{ ref('int_bubble__slot_class_section') }} scs
        on p.slot_class_section_id = scs.slot_class_section_id
    where p.status = 'Cancelled'
      and scs.is_active and not scs.is_removed
),

slots as (
    select
        slot_id,
        school_id,
        case
            when start_time is null or end_time is null or end_time <= start_time then 1.0
            else extract(epoch from (end_time - start_time)) / 3600.0
        end as slot_hours,
        (start_time is null or end_time is null or end_time <= start_time) as slot_times_missing
    from {{ ref('int_bubble__slot') }}
),

holiday_reasons as (
    select
        cd.slot_id,
        cd.class_section_id,
        cd.session_date,
        min(sh.holiday_reason) as cancel_reason
    from cancelled_dates cd
    join slots s on cd.slot_id = s.slot_id
    left join {{ ref('int_bubble__school_holiday') }} sh
        on s.school_id = sh.school_id
       and cd.session_date between sh.start_date and sh.end_date
       and sh.is_removed = false
    group by 1, 2, 3
)

select
    cd.chapter_id,
    cd.slot_id,
    cd.class_section_id,
    cd.session_date,
    date_trunc('week', cd.session_date)::date as week_start,
    case
        when cx.slot_id is not null and not coalesce(at.has_present or at.has_substitute, false) then 'cancelled'
        when coalesce(at.has_present, false) then 'held_by_original'
        when coalesce(at.has_substitute, false) then 'held_by_substitute'
        when at.slot_id is not null then 'volunteer_absent'
        else 'no_attendance'
    end as session_group,
    case
        when cx.slot_id is not null and not coalesce(at.has_present or at.has_substitute, false)
        then coalesce(hr.cancel_reason, 'Reason not logged')
    end as cancel_reason,
    s.slot_hours,
    s.slot_times_missing
from circle_dates cd
join slots s on cd.slot_id = s.slot_id
left join attendance at
    on cd.slot_id = at.slot_id
   and cd.class_section_id = at.class_section_id
   and cd.session_date = at.session_date
left join cancelled_dates cx
    on cd.slot_id = cx.slot_id
   and cd.class_section_id = cx.class_section_id
   and cd.session_date = cx.session_date
left join holiday_reasons hr
    on cd.slot_id = hr.slot_id
   and cd.class_section_id = hr.class_section_id
   and cd.session_date = hr.session_date
