{{ config(materialized='table') }}

-- fct_e2_planned_session_status: per-date classification of every planned session for a scheduled
-- (slotted) class_section
-- Grain: one row per (slot_class_section_id, academic_year, planned_date)
-- Feeds fct_e2_sessions_summary's classes_conducted / classes_cancelled /
-- classes_with_volunteer_absenteeism / classes_without_assigned_volunteer / classes_not_yet_due /
-- classes_unexplained_other breakdown -- this is the historically-anchored replacement for the old
-- "today's snapshot x whole window" approach, which silently rewrote past weeks every time a
-- volunteer got (re)assigned.
-- Scope: only slotted sections (a real scheduled day_of_week) get a row here -- a section with NO
-- slot at all has no real schedule to project dates from, so it's handled separately in
-- fct_e2_sessions_summary via the coarser total_sections-based estimate, split by elapsed/remaining
-- weeks the same way, just without per-date precision.
-- planned_date walk: same weekly day-of-week projection fct_e2_cancellations already uses, but
-- sourced from int_bubble__slot directly (not fct_e2_volunteer_allocation_history), so it also
-- covers sections that never had a volunteer allocated at all -- not just ones with allocation
-- history.
-- Status classification, in priority order (2026-09-12 design):
--   1. Conducted -- a real attendance record shows the volunteer present, OR absent with a valid
--      substitute logged. Checked first, regardless of timing: this is concrete proof the class
--      happened, no need to wait for anything.
--   2. Volunteer Absent (explicit) -- a real attendance record shows the volunteer absent with NO
--      substitute. Also concrete evidence, no need to wait for the grace period either.
--   3. Not Yet Due -- no attendance record exists yet, and today is still within this session's own
--      7-day grace period (planned_date + 7 days, matching the DOTS submission window: a Monday
--      4-5pm slot is due for submission by the following Monday 4pm). Covers genuinely future
--      sessions too, since today < planned_date + 7 is trivially true for any future planned_date.
--   4. Cancelled -- grace period passed, still no record, and planned_date falls in a school_holiday
--      window for that school (same match fct_e2_cancellations uses) -- checked before staffing, on
--      purpose: holiday status is decided independently of whether the section happened to have a
--      volunteer, matching fct_e2_cancellations' own long-standing semantics (a holiday cancels the
--      day regardless of staffing). Confirmed 2026-09-12: without this ordering, every one of a real
--      36-row sample of holiday-matched dates fell into "No Volunteer Assigned" instead, since they
--      also happened to be unstaffed at the time.
--   5. No Volunteer Assigned -- grace period passed, still no record, not a holiday, and no volunteer
--      assignment was active on this section as of planned_date (built from
--      int_bubble__slot_class_section_volunteer's created_date/deleted_at, so it correctly
--      reconstructs multiple staffing gaps over time, not just a single before/after-first-assignment
--      cutoff).
--   6. Volunteer Absent (implicit) -- grace period passed, still no record, was staffed, not a
--      holiday: nobody ever submitted anything. Treated the same as an explicit absence, per
--      2026-09-12 decision -- silence past the grace deadline IS the "did not happen" signal, not a
--      separate "unexplained" state.
-- What's left in classes_unexplained_other downstream is therefore only genuine data/model integrity
-- gaps (e.g. a malformed day_of_week preventing this walk from running for a section at all) -- not
-- a normal outcome of this classification.

with slotted_sections as (
    select
        scs.slot_class_section_id,
        s.school_id,
        s.school_academic_year_id,
        s.day_of_week
    from {{ ref('int_bubble__slot_class_section') }} scs
    join {{ ref('int_bubble__slot') }} s
        on scs.slot_id = s.slot_id
        and s.is_removed = false
    where scs.is_removed = false
),

sections_with_dow as (
    select
        *,
        case upper(trim(day_of_week))
            when 'SUNDAY' then 0
            when 'MONDAY' then 1
            when 'TUESDAY' then 2
            when 'WEDNESDAY' then 3
            when 'THURSDAY' then 4
            when 'FRIDAY' then 5
            when 'SATURDAY' then 6
            else null
        end as slot_dow
    from slotted_sections
),

sections_with_window as (
    select
        swd.slot_class_section_id,
        swd.school_id,
        w.academic_year,
        swd.slot_dow,
        w.window_start_date,
        w.window_end_date
    from sections_with_dow swd
    join {{ ref('dim_school_academic_year_window') }} w
        on swd.school_academic_year_id = w.school_academic_year_id
    where w.window_start_date is not null
        and w.window_end_date is not null
        and swd.slot_dow is not null
),

first_planned_date as (
    select
        *,
        window_start_date
            + ((slot_dow - extract(dow from window_start_date)::int + 7) % 7) as first_session_date
    from sections_with_window
),

planned_dates as (
    select
        fpd.slot_class_section_id,
        fpd.school_id,
        fpd.academic_year,
        (fpd.first_session_date + (gs.n * 7))::date as planned_date
    from first_planned_date fpd
    cross join generate_series(0, 60) as gs (n)
    where fpd.first_session_date + (gs.n * 7) <= fpd.window_end_date
),

is_staffed_per_date as (
    select
        pd.slot_class_section_id,
        pd.planned_date,
        exists (
            select 1
            from {{ ref('int_bubble__slot_class_section_volunteer') }} st
            where st.slot_class_section_id = pd.slot_class_section_id
                and st.created_date <= pd.planned_date
                and (st.is_removed = false or st.deleted_at::date > pd.planned_date)
        ) as is_staffed
    from planned_dates pd
),

holiday_match as (
    select distinct
        pd.slot_class_section_id,
        pd.planned_date
    from planned_dates pd
    join {{ ref('int_bubble__school_holiday') }} sh
        on pd.school_id = sh.school_id
        and pd.planned_date >= sh.start_date
        and pd.planned_date <= sh.end_date
        and sh.is_removed = false
),

attendance_per_date as (
    select
        slot_class_section_id,
        date_of_slot::date as planned_date,
        bool_or(is_substitute is not null) as has_conducted_record,
        bool_or(is_substitute is null) as has_unresolved_absence_record
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }}
    group by slot_class_section_id, date_of_slot::date
)

select
    pd.slot_class_section_id,
    pd.school_id,
    pd.academic_year,
    pd.planned_date,
    (pd.planned_date + interval '7 days')::date as grace_deadline,
    case
        when att.has_conducted_record then 'Conducted'
        when att.has_unresolved_absence_record then 'Volunteer Absent'
        when current_date < (pd.planned_date + interval '7 days')::date then 'Not Yet Due'
        when hm.slot_class_section_id is not null then 'Cancelled'
        when not coalesce(isd.is_staffed, false) then 'No Volunteer Assigned'
        else 'Volunteer Absent'
    end as status
from planned_dates pd
left join is_staffed_per_date isd
    on pd.slot_class_section_id = isd.slot_class_section_id
    and pd.planned_date = isd.planned_date
left join holiday_match hm
    on pd.slot_class_section_id = hm.slot_class_section_id
    and pd.planned_date = hm.planned_date
left join attendance_per_date att
    on pd.slot_class_section_id = att.slot_class_section_id
    and pd.planned_date = att.planned_date
