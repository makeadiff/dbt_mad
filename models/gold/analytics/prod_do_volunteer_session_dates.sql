{{ config(materialized='table') }}

-- DO dashboard (E2): every planned volunteer session from 2026-10-01 (tracking start, spec §3.3),
-- with whether it was taken or held. Base for prod_do_volunteer_consistency (Metric 3). The weekly
-- view (§3.3b) counts per circle from prod_do_mentoring_circles instead. Not read by Dalgo.
--
-- Planned: fct_e2_volunteer_consistency's logic, as dates instead of a count. One session per week
-- on the slot's weekday per allocation (fct_e2_volunteer_allocation_history), from the later of
-- the slot-link start, the term start and 2026-10-01, to the earlier of the link end (or AY end)
-- and today. Dates marked Cancelled in fct_e2_planned_session_status (a school holiday with no
-- session logged) are dropped. That model is used instead of fct_e2_cancellations because the
-- latter is a whole-year total with no dates, so it can't be clipped to a period.
--
-- Taken (Metric 3): the volunteer is marked present and isn't a substitute. Held (§3.3b): taken,
-- or absent with a substitute logged. fct_e2_volunteer_consistency's attended count is not reused:
-- it counts absent records too (spec §6).
--
-- Today is the IST date: the nightly job runs at 01:36 IST, when the UTC date is still yesterday.
--
-- Grain: one row per (volunteer_id, slot_class_section_id, session_date).

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

allocations as (
    select
        a.volunteer_id,
        a.slot_class_section_id,
        a.partner_id::text as chapter_id,
        case upper(trim(a.day_of_week))
            when 'SUNDAY' then 0
            when 'MONDAY' then 1
            when 'TUESDAY' then 2
            when 'WEDNESDAY' then 3
            when 'THURSDAY' then 4
            when 'FRIDAY' then 5
            when 'SATURDAY' then 6
        end as slot_dow,
        greatest(a.slot_class_sec_start_date::date, a.sessions_start_date::date, p.tracking_start) as window_start,
        case
            when a.is_active then least(p.today, coalesce(a.slot_class_sec_end_date::date, '2027-04-30'::date))
            else least(p.today, coalesce(a.slot_class_sec_end_date::date, p.today))
        end as window_end
    from {{ ref('fct_e2_volunteer_allocation_history') }} a
    cross join params p
    where a.academic_year = '2026-2027'
),

planned_dates as (
    select distinct
        al.chapter_id,
        al.volunteer_id,
        al.slot_class_section_id,
        d::date as session_date
    from allocations al
    cross join lateral generate_series(
        al.window_start + ((al.slot_dow - extract(dow from al.window_start)::int + 7) % 7),
        al.window_end,
        interval '7 days'
    ) as d
    where al.slot_dow is not null
      and al.window_start is not null
      and al.window_end is not null
),

cancelled_dates as (
    select distinct slot_class_section_id, planned_date
    from {{ ref('fct_e2_planned_session_status') }}
    where status = 'Cancelled'
),

attendance as (
    select
        volunteer_id,
        slot_class_section_id,
        date_of_slot::date as session_date,
        bool_or(upper(trim(attendance)) = 'TRUE' and is_substitute = false) as is_taken,
        bool_or(is_substitute) as is_by_substitute
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    where academic_year = '2026-2027'
    group by 1, 2, 3
)

select
    pd.chapter_id,
    pd.volunteer_id,
    pd.slot_class_section_id,
    pd.session_date,
    date_trunc('week', pd.session_date)::date as week_start,
    coalesce(at.is_taken, false) as is_taken,
    coalesce(at.is_by_substitute, false) and not coalesce(at.is_taken, false) as is_by_substitute,
    coalesce(at.is_taken, false) or coalesce(at.is_by_substitute, false) as is_held
from planned_dates pd
left join cancelled_dates cd
    on pd.slot_class_section_id = cd.slot_class_section_id
   and pd.session_date = cd.planned_date
left join attendance at
    on pd.volunteer_id = at.volunteer_id
   and pd.slot_class_section_id = at.slot_class_section_id
   and pd.session_date = at.session_date
where cd.slot_class_section_id is null
