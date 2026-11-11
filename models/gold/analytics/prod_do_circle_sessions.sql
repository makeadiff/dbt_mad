{{ config(materialized='table') }}

-- DO dashboard (E2): every planned circle-date from 2026-10-01, cancelled ones kept in, each in
-- exactly one group (spec §3.7 R2 as amended by §3.8 rulings 1 and 2). Base for
-- prod_do_weekly_sessions, prod_do_session_breakdown, prod_do_cancellation_reasons and
-- prod_do_child_sessions, so the planned, held and group rules live in one place. Not read by Dalgo.
--
-- Planned (§3.8 ruling 2): every circle with a slot (prod_do_mentoring_circles), one session a week
-- on its slot day, from 2026-10-01 to today, whether or not it has a volunteer, ignoring the
-- slot-link start and the allocation date. Cancelled dates are kept here; the weekly model drops them.
-- Groups, first that applies:
--   cancelled             a cancellation logged (fct_e2_planned_session_status 'Cancelled'), nothing held
--   held_by_original      a volunteer present
--   held_by_substitute    no volunteer present, a substitute logged
--   volunteer_absent      a volunteer was allocated to the circle on that date and nothing was held:
--                         marked absent with no substitute, or nothing logged at all
--   no_volunteer_assigned nothing logged and no volunteer allocated to the circle on that date
-- "No attendance logged" is no longer a group (§3.8 ruling 1): a silent date is the volunteer's
-- absence when one was allocated, and a staffing gap when none was.
-- Allocated on a date: a slot_class_section_volunteer row on any of the pair's slot_class_section
-- rows with created_date on or before the date and either not removed or removed after it
-- (deleted_at), the same reconstruction fct_e2_planned_session_status uses for "No Volunteer
-- Assigned". Cancelled dates with something held count as held, as before, so planned minus
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
    select chapter_id, slot_id, class_section_id, slot_dow
    from {{ ref('prod_do_mentoring_circles') }}
    where slot_dow is not null
),

circle_dates as (
    select c.chapter_id, c.slot_id, c.class_section_id, d::date as session_date
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

-- Volunteer allocations on the pair's rows, as date ranges (end open while the row isn't removed).
allocations as (
    select
        scs.slot_id,
        scs.class_section_id,
        v.created_date::date as allocated_from,
        v.is_removed,
        v.deleted_at::date as removed_on
    from {{ ref('int_bubble__slot_class_section_volunteer') }} v
    join {{ ref('int_bubble__slot_class_section') }} scs
        on v.slot_class_section_id = scs.slot_class_section_id
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
),

flagged as (
    select
        cd.chapter_id,
        cd.slot_id,
        cd.class_section_id,
        cd.session_date,
        cx.slot_id is not null as is_cancelled,
        coalesce(at.has_present, false) as has_present,
        coalesce(at.has_substitute, false) as has_substitute,
        at.slot_id is not null as has_attendance_record,
        exists (
            select 1
            from allocations a
            where a.slot_id = cd.slot_id
              and a.class_section_id = cd.class_section_id
              and a.allocated_from <= cd.session_date
              and (not a.is_removed or a.removed_on > cd.session_date)
        ) as is_volunteer_allocated
    from circle_dates cd
    left join attendance at
        on cd.slot_id = at.slot_id
       and cd.class_section_id = at.class_section_id
       and cd.session_date = at.session_date
    left join cancelled_dates cx
        on cd.slot_id = cx.slot_id
       and cd.class_section_id = cx.class_section_id
       and cd.session_date = cx.session_date
)

select
    f.chapter_id,
    f.slot_id,
    f.class_section_id,
    f.session_date,
    date_trunc('week', f.session_date)::date as week_start,
    case
        when f.is_cancelled and not (f.has_present or f.has_substitute) then 'cancelled'
        when f.has_present then 'held_by_original'
        when f.has_substitute then 'held_by_substitute'
        when f.has_attendance_record or f.is_volunteer_allocated then 'volunteer_absent'
        else 'no_volunteer_assigned'
    end as session_group,
    f.is_volunteer_allocated,
    case
        when f.is_cancelled and not (f.has_present or f.has_substitute)
        then coalesce(hr.cancel_reason, 'Reason not logged')
    end as cancel_reason,
    s.slot_hours,
    s.slot_times_missing
from flagged f
join slots s on f.slot_id = s.slot_id
left join holiday_reasons hr
    on f.slot_id = hr.slot_id
   and f.class_section_id = hr.class_section_id
   and f.session_date = hr.session_date
