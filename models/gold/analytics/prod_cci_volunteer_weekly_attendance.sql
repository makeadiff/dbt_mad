{{ config(materialized='table') }}

-- CCI dashboard (E1): one row per volunteer per CCI per week (DO spec §3.9 W1), so volunteer
-- attendance follows the Dalgo Week and Month filters. Same planned and taken rules as
-- prod_cci_volunteer_sessions (read its header for the slot-day basis, the holiday and cancellation
-- drops, and the day-early attendance match); only the time grain changes. Weeks and months come
-- from prod_cci_weekly_sessions, the same source as prod_cci_session_breakdown, so Week / Week
-- starting / Month / Month starting match prod_cci_session_breakdown_display exactly. Sessions count
-- from 2026-10-01, so the weeks of a person add up to their "Since 1 October" row in
-- prod_cci_volunteer_sessions (test).
--
-- A row needs planned >= 1 or taken >= 1, as prod_cci_volunteer_sessions: a few volunteers take
-- sessions on circles they aren't listed on today, and dropping those weeks would break the tie to
-- the term total. Planned sessions taken = planned dates the volunteer took (never more than
-- planned), so Dalgo's "% of planned classes taken" is SUM planned taken / SUM planned, the same
-- numerator as the Period display's %.
--
-- Grain: one row per (chapter_id, volunteer_user_id, week_start).

with weeks as (
    select
        week_start,
        week_label,
        month_start,
        month_label,
        greatest(week_start, '2026-10-01'::date) as range_start,
        least(week_start + 6, (now() at time zone 'Asia/Kolkata')::date) as range_end
    from {{ ref('prod_cci_weekly_sessions') }}
    group by 1, 2, 3, 4
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
        wk.week_start,
        p.chapter_id,
        p.volunteer_user_id,
        count(*) as sessions_planned,
        count(*) filter (where p.is_taken) as planned_taken
    from weeks wk
    join planned p
        on p.session_date between wk.range_start and wk.range_end
    group by 1, 2, 3
),

taken_per_person as (
    select
        wk.week_start,
        t.chapter_id,
        t.volunteer_user_id,
        count(*) as sessions_taken
    from weeks wk
    join taken t
        on t.session_date between wk.range_start and wk.range_end
    group by 1, 2, 3
),

per_person as (
    select
        coalesce(pp.week_start, tp.week_start) as week_start,
        coalesce(pp.chapter_id, tp.chapter_id) as chapter_id,
        coalesce(pp.volunteer_user_id, tp.volunteer_user_id) as volunteer_user_id,
        coalesce(pp.sessions_planned, 0) as sessions_planned,
        coalesce(pp.planned_taken, 0) as planned_taken,
        coalesce(tp.sessions_taken, 0) as sessions_taken
    from planned_per_person pp
    full outer join taken_per_person tp
        on pp.week_start = tp.week_start
       and pp.chapter_id = tp.chapter_id
       and pp.volunteer_user_id = tp.volunteer_user_id
),

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
    wk.week_start,
    wk.week_label,
    wk.month_start,
    wk.month_label,
    pp.volunteer_user_id,
    coalesce(n.volunteer_name, 'Volunteer ' || pp.volunteer_user_id) as volunteer_name,
    pp.sessions_planned,
    pp.sessions_taken,
    pp.planned_taken
from per_person pp
join weeks wk
    on pp.week_start = wk.week_start
join {{ ref('prod_cci_chapter_summary') }} s
    on pp.chapter_id = s.chapter_id
left join names n
    on pp.volunteer_user_id = n.volunteer_user_id
