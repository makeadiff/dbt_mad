{{ config(materialized='table') }}

-- DO dashboard (E2): one row per volunteer per chapter per period (spec §3.7 R4, volunteer table).
-- Planned and taken are Metric 3's (prod_do_volunteer_session_dates): a volunteer in two circles
-- has their sessions combined within the chapter. Attendance % = taken / planned. Status:
-- Healthy >= 90%, At risk 75-89%, Unhealthy < 75%. Hours contributed = sessions taken x the
-- slot's length (int_bubble__slot; 1 hour when a time is missing, R5). Only volunteers with >= 1
-- planned session in the period get a row. Periods are in macros/session_periods.sql.
--
-- Grain: one row per (chapter_id, volunteer_id, period_type, period_start).

with periods as (
    {{ session_periods() }}
),

-- The volunteer's name comes from their Bubble user record (the volunteer_id the allocation points
-- to), not from attendance, which only names volunteers who have a record.
names as (
    select volunteer_id, max(nullif(trim(user_display_name), '')) as volunteer_name
    from {{ ref('int_bubble__user') }}
    group by 1
),

slot_hours as (
    select
        scs.slot_class_section_id,
        case
            when s.start_time is null or s.end_time is null or s.end_time <= s.start_time then 1.0
            else extract(epoch from (s.end_time - s.start_time)) / 3600.0
        end as slot_hours
    from {{ ref('int_bubble__slot_class_section') }} scs
    join {{ ref('int_bubble__slot') }} s on scs.slot_id = s.slot_id
),

per_person as (
    select
        pr.period_type,
        pr.period_label,
        pr.period_sort,
        pr.period_start,
        pr.period_end,
        sd.chapter_id,
        sd.volunteer_id,
        count(*) as sessions_planned,
        count(*) filter (where sd.is_taken) as sessions_taken,
        coalesce(sum(coalesce(sh.slot_hours, 1.0)) filter (where sd.is_taken), 0) as hours_contributed
    from periods pr
    join {{ ref('prod_do_volunteer_session_dates') }} sd
        on sd.session_date between pr.period_start and pr.period_end
    left join slot_hours sh
        on sd.slot_class_section_id = sh.slot_class_section_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    ch.engine,
    ch.chapter_id,
    ch.chapter_name,
    ch.city,
    ch.co_name,
    ch.cho_name,
    ch.district,
    ch.do_name,
    pp.period_type,
    pp.period_label,
    pp.period_sort,
    pp.period_start,
    pp.period_end,
    pp.volunteer_id,
    coalesce(n.volunteer_name, 'Volunteer ' || pp.volunteer_id) as volunteer_name,
    pp.sessions_planned,
    pp.sessions_taken,
    round(100.0 * pp.sessions_taken / pp.sessions_planned, 1) as attendance_pct,
    round(pp.hours_contributed::numeric, 1) as hours_contributed,
    case
        when pp.sessions_taken::numeric / pp.sessions_planned >= 0.90 then 'Healthy'
        when pp.sessions_taken::numeric / pp.sessions_planned >= 0.75 then 'At risk'
        else 'Unhealthy'
    end as status
from per_person pp
join {{ ref('prod_do_chapter_summary') }} ch
    on pp.chapter_id = ch.chapter_id
left join names n
    on pp.volunteer_id = n.volunteer_id
