{{ config(materialized='table') }}

-- CCI dashboard (E1): one row per volunteer per CCI per period, with planned, taken, % taken and
-- status, the same columns as prod_do_volunteer_sessions (spec E1 §8 2026-11-12 (later) ruling 2,
-- §3.10, §7 L1). A volunteer in two circles has their sessions combined within the CCI. Periods are
-- in macros/session_periods.sql.
--
-- Slot-day basis: Platform Commons generates every CCI session in advance, but those generated
-- sessions are not in the warehouse (checked 2026-11-12: no pc_raw or pc_reporting table has one row
-- per circle per date with the allocated volunteer). Until they land, planned is rebuilt from the
-- slot day, as prod_cci_planned_sessions does.
--
-- Planned: the circle-dates in prod_cci_planned_sessions for every circle the volunteer is on today
-- (stg_pc_worknode_slot_shift_user_list through the shift-user bridge, the list that
-- prod_cci_mentoring_circles.volunteers counts). Platform Commons has no allocation dates, so this is
-- the current list, as L1 does for "no volunteer assigned". Dropped, as School drops a holiday with
-- no session logged (prod_do_volunteer_session_dates): dates in the 'cancelled' group (an approved
-- cancellation, nothing held) and dates inside an active HOLIDAY event on the circle's batch
-- (stg_pc_sc_level_batch_events) with nothing held.
--
-- Taken: the volunteer marked present on any CCI circle's session (int_pc_volunteer_attendance), as
-- before, so the column still adds up to the "sessions taken" tile. Hours contributed = sessions taken
-- x the slot's length (prod_cci_circle_sessions; 1 hour when a time is missing).
--
-- % of planned sessions taken = planned dates the volunteer took / planned, 1 decimal. A taken session
-- counts against a planned date on the same circle when attendance is on that date or the day before
-- (attendance dates run a day early, spec §6); it is then dated on the planned date, so both land in
-- the same period. Taken sessions on a circle the volunteer isn't listed on
-- today, or on a date that isn't planned, show in Sessions taken but not in the %. Status: Healthy
-- 90%+, At risk 75-89%, Unhealthy under 75% (prod_do_volunteer_sessions' CASE). A row needs planned >= 1
-- or taken >= 1; when planned is 0, % and status are null.
--
-- Volunteer key: the Platform Commons user ID, which both the circle list (owner_user_id) and
-- attendance (tagged_volunteer_user_id) carry.
--
-- Grain: one row per (chapter_id, volunteer_user_id, period_type, period_start).

with periods as (
    {{ session_periods() }}
),

circles as (
    select chapter_id, worknode_slot_shift_id, sc_level_batch_id
    from {{ ref('prod_cci_mentoring_circles') }}
),

circle_volunteers as (
    select distinct b.worknode_slot_shift_id, ul.owner_user_id::text as volunteer_user_id
    from {{ ref('stg_pc_worknode_slot_shift_user_bridge') }} b
    join {{ ref('stg_pc_worknode_slot_shift_user_list') }} ul
        on b.slot_shift_user_id = ul.slot_shift_user_id
    where ul.is_active
      and ul.owner_user_id is not null
),

holidays as (
    select distinct br.sc_level_batch_id, e.start_date, e.end_date
    from {{ ref('stg_pc_sc_level_batch_events') }} e
    join {{ ref('stg_pc_sc_level_batch_events_bridge') }} br
        on e.batch_event_id = br.batch_event_id
    where e.event_type = 'SCLEVELBATCH_EVENT.HOLIDAY'
      and coalesce(e.is_active, true)
),

planned_circle_dates as (
    select p.chapter_id, p.worknode_slot_shift_id, p.session_date
    from {{ ref('prod_cci_planned_sessions') }} p
    join circles c
        on p.worknode_slot_shift_id = c.worknode_slot_shift_id
    where p.session_group <> 'cancelled'
      and not (
          p.session_group not in ('held_by_original', 'held_by_substitute')
          and exists (
              select 1 from holidays h
              where h.sc_level_batch_id = c.sc_level_batch_id
                and p.session_date between h.start_date and h.end_date
          )
      )
),

taken_raw as (
    select distinct
        c.chapter_id,
        a.tagged_volunteer_user_id::text as volunteer_user_id,
        a.section_slot_shift_id as worknode_slot_shift_id,
        a.scheduled_session_date::date as attendance_date
    from {{ ref('int_pc_volunteer_attendance') }} a
    join circles c
        on a.section_slot_shift_id = c.worknode_slot_shift_id
    where a.attendance_status = 'PRESENT'
      and a.tagged_volunteer_user_id is not null
      and a.scheduled_session_date::date >= date '2026-09-30'
),

-- A taken session is dated on the circle's slot day (its planned date) when attendance falls on that
-- day or the day before, so planned and taken land in the same period; otherwise on the attendance
-- date. Only sessions dated from 1 Oct count.
taken as (
    select
        tr.chapter_id,
        tr.volunteer_user_id,
        tr.worknode_slot_shift_id,
        tr.attendance_date,
        coalesce(
            (select min(p.session_date) from {{ ref('prod_cci_planned_sessions') }} p
             where p.worknode_slot_shift_id = tr.worknode_slot_shift_id
               and tr.attendance_date in (p.session_date, p.session_date - 1)),
            tr.attendance_date
        ) as session_date
    from taken_raw tr
),

planned as (
    select
        pcd.chapter_id,
        cv.volunteer_user_id,
        pcd.worknode_slot_shift_id,
        pcd.session_date,
        exists (
            select 1 from taken t
            where t.volunteer_user_id = cv.volunteer_user_id
              and t.worknode_slot_shift_id = pcd.worknode_slot_shift_id
              and t.session_date = pcd.session_date
        ) as is_taken
    from planned_circle_dates pcd
    join circle_volunteers cv
        on pcd.worknode_slot_shift_id = cv.worknode_slot_shift_id
),

planned_per_person as (
    select
        pr.period_type,
        pr.period_start,
        p.chapter_id,
        p.volunteer_user_id,
        count(*) as sessions_planned,
        count(*) filter (where p.is_taken) as planned_taken
    from periods pr
    join planned p
        on p.session_date between pr.period_start and pr.period_end
    group by 1, 2, 3, 4
),

taken_per_person as (
    select
        pr.period_type,
        pr.period_start,
        t.chapter_id,
        t.volunteer_user_id,
        count(*) as sessions_taken,
        sum(coalesce(cs.slot_hours, 1.0)) as hours_contributed
    from periods pr
    join taken t
        on t.session_date between pr.period_start and pr.period_end
       and t.session_date >= date '2026-10-01'
    left join {{ ref('prod_cci_circle_sessions') }} cs
        on t.worknode_slot_shift_id = cs.worknode_slot_shift_id
       and t.attendance_date = cs.session_date
    group by 1, 2, 3, 4
),

per_person as (
    select
        coalesce(pp.period_type, tp.period_type) as period_type,
        coalesce(pp.period_start, tp.period_start) as period_start,
        coalesce(pp.chapter_id, tp.chapter_id) as chapter_id,
        coalesce(pp.volunteer_user_id, tp.volunteer_user_id) as volunteer_user_id,
        coalesce(pp.sessions_planned, 0) as sessions_planned,
        coalesce(pp.planned_taken, 0) as planned_taken,
        coalesce(tp.sessions_taken, 0) as sessions_taken,
        coalesce(tp.hours_contributed, 0) as hours_contributed
    from planned_per_person pp
    full outer join taken_per_person tp
        on pp.period_type = tp.period_type
       and pp.period_start = tp.period_start
       and pp.chapter_id = tp.chapter_id
       and pp.volunteer_user_id = tp.volunteer_user_id
),

-- The volunteer's name comes from their Platform Commons user record.
names as (
    select u.user_id::text as volunteer_user_id,
           max(nullif(trim(u.first_name || ' ' || coalesce(u.last_name, '')), '')) as volunteer_name
    from {{ ref('stg_pc_user') }} u
    where u.user_id::text in (select volunteer_user_id from per_person)
    group by 1
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    pr.period_type,
    pr.period_label,
    pr.period_sort,
    pr.period_start,
    pr.period_end,
    pp.volunteer_user_id,
    coalesce(n.volunteer_name, 'Volunteer ' || pp.volunteer_user_id) as volunteer_name,
    pp.sessions_planned,
    pp.sessions_taken,
    pp.planned_taken,
    case when pp.sessions_planned > 0
        then round(100.0 * pp.planned_taken / pp.sessions_planned, 1)
    end as attendance_pct,
    round(pp.hours_contributed::numeric, 1) as hours_contributed,
    case
        when pp.sessions_planned = 0 then null
        when pp.planned_taken::numeric / pp.sessions_planned >= 0.90 then 'Healthy'
        when pp.planned_taken::numeric / pp.sessions_planned >= 0.75 then 'At risk'
        else 'Unhealthy'
    end as status
from per_person pp
join periods pr
    on pp.period_type = pr.period_type
   and pp.period_start = pr.period_start
join {{ ref('prod_cci_chapter_summary') }} s
    on pp.chapter_id = s.chapter_id
left join names n
    on pp.volunteer_user_id = n.volunteer_user_id
