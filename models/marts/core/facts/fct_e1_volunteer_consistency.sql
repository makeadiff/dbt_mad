{{ config(materialized='table') }}

-- fct_e1_volunteer_consistency: per-volunteer session consistency (planned vs. attended) for one
-- academic year, for E1 (session-ops / platform_commons). E1 counterpart to
-- fct_e2_volunteer_consistency.
-- Grain: one row per (volunteer_id, school_id, academic_year) -- no chapter_id for E1, matching
-- fct_e1_school_coverage/fct_e1_session_summary's existing choice (platform_commons has no
-- chapter/CO/CHO concept, only schools).
--
-- planned_sessions is the same coarser estimate fct_e1_session_summary already uses for schools --
-- {{ var('e1_planned_sessions_per_week', 2) }} assumed sessions/week per tagged slot_shift, over the
-- same fixed academic-year window (July 1 - March 30 of the AY, capped at today), minus that
-- slot_shift's approved cancellations. Computed per volunteer's own tagged slot_shift(s) rather than
-- per school's total sections. This is a known, accepted tradeoff, not a gap unique to this model:
-- platform_commons' worknodeSlotShiftUserList (the volunteer-tagging table) has no creation
-- timestamp at all -- only isActive/xDeletedTimestamp for when a volunteer was untagged -- so unlike
-- E2 (which knows a volunteer's exact allocation start date from slot_class_section_volunteer's own
-- created_date), there's no way to know precisely when a given volunteer started on a given
-- slot_shift. This will overestimate planned_sessions for a volunteer who joined partway through the
-- year, same as fct_e1_session_summary's own documented tradeoff.
--
-- attended_sessions/original_sessions/substitute_sessions come from int_pc_volunteer_attendance,
-- which has clean attendance_status (PRESENT/ABSENT) and substitution fields -- better structured
-- than E2's DOTS-sourced text fields, so this doesn't need E2's is_substitute text-parsing logic.
-- A PRESENT row counts as a substitute_session when an approved SUBSTITUTE event exists for that
-- (slot_shift, date) and the attending volunteer (tagged_volunteer_user_id) is that event's assignee
-- (by_user_id) -- i.e. this volunteer covered someone else's slot. Otherwise it's an original_session.

with volunteer_slot_shifts as (
    -- One row per volunteer tagged to one slot_shift for one school/academic_year.
    select
        school_id,
        academic_year,
        worknode_slot_shift_id,
        owner_user_id::bigint as volunteer_id,
        bool_or(volunteer_tag_is_active) as is_active
    from {{ ref('int_pc_batch_coverage') }}
    where owner_user_id is not null
      and worknode_slot_shift_id is not null
    group by school_id, academic_year, worknode_slot_shift_id, owner_user_id
),

-- Same fixed academic-year window as fct_e1_session_summary: July 1 of the AY's first year
-- through March 30 of its second year, capped at today.
planned_sessions_window as (
    select distinct
        academic_year,
        case when academic_year ~ '^[0-9]{4}-[0-9]{4}$'
            then make_date(split_part(academic_year, '-', 1)::int, 7, 1)
        end as window_start,
        case when academic_year ~ '^[0-9]{4}-[0-9]{4}$'
            then least(current_date, make_date(split_part(academic_year, '-', 2)::int, 3, 30))
        end as window_end
    from volunteer_slot_shifts
),

weeks_per_academic_year as (
    select
        academic_year,
        greatest(floor((window_end - window_start) / 7.0)::int, 0) as weeks_in_window
    from planned_sessions_window
),

-- Approved cancellation events per slot_shift -- same APPROVED-only filter fct_e1_session_summary
-- and fct_e1_cancellation_reasons apply.
cancellations_per_slot_shift as (
    select
        for_slot_shift_id as worknode_slot_shift_id,
        count(*) as total_cancellations
    from {{ ref('stg_pc_substitute') }}
    where request_status = 'SLOT_SHIFT_SUBSTITUTE_REQ_STATUS.APPROVED'
      and request_type = 'SLOT_SHIFT_SUBSTITUTE_REQ_TYPE.CANCELLATION'
    group by 1
),

planned_sessions as (
    select
        vss.school_id,
        vss.academic_year,
        vss.worknode_slot_shift_id,
        vss.volunteer_id,
        vss.is_active,
        greatest(
            ({{ var('e1_planned_sessions_per_week', 2) }} * wpay.weeks_in_window)
                - coalesce(cps.total_cancellations, 0),
            0
        ) as planned_sessions
    from volunteer_slot_shifts vss
    left join weeks_per_academic_year wpay
        on vss.academic_year = wpay.academic_year
    left join cancellations_per_slot_shift cps
        on vss.worknode_slot_shift_id = cps.worknode_slot_shift_id
),

-- Approved substitute (not cancellation) events per (slot_shift, date), for the original vs.
-- substitute split below.
substitute_events as (
    select
        for_slot_shift_id,
        for_date::date as event_date,
        by_user_id as assignee_user_id
    from {{ ref('stg_pc_substitute') }}
    where request_status = 'SLOT_SHIFT_SUBSTITUTE_REQ_STATUS.APPROVED'
      and request_type = 'SLOT_SHIFT_SUBSTITUTE_REQ_TYPE.SUBSTITUTE'
),

attendance_agg as (
    select
        va.tagged_volunteer_user_id as volunteer_id,
        va.section_slot_shift_id as worknode_slot_shift_id,
        count(distinct va.scheduled_session_date) as attended_sessions,
        count(distinct va.scheduled_session_date) filter (
            where se.assignee_user_id is null or se.assignee_user_id <> va.tagged_volunteer_user_id
        ) as original_sessions,
        count(distinct va.scheduled_session_date) filter (
            where se.assignee_user_id = va.tagged_volunteer_user_id
        ) as substitute_sessions
    from {{ ref('int_pc_volunteer_attendance') }} va
    left join substitute_events se
        on va.section_slot_shift_id = se.for_slot_shift_id
        and va.scheduled_session_date::date = se.event_date
    where va.attendance_status = 'PRESENT'
      and va.tagged_volunteer_user_id is not null
    group by va.tagged_volunteer_user_id, va.section_slot_shift_id
),

joined as (
    select
        ps.volunteer_id,
        ps.school_id,
        ps.academic_year,
        bool_or(ps.is_active) as is_active,
        sum(ps.planned_sessions) as planned_sessions,
        coalesce(sum(aa.attended_sessions), 0) as attended_sessions,
        coalesce(sum(aa.original_sessions), 0) as original_sessions,
        coalesce(sum(aa.substitute_sessions), 0) as substitute_sessions
    from planned_sessions ps
    left join attendance_agg aa
        on ps.volunteer_id = aa.volunteer_id
        and ps.worknode_slot_shift_id = aa.worknode_slot_shift_id
    group by ps.volunteer_id, ps.school_id, ps.academic_year
)

select
    j.volunteer_id,
    u.first_name || ' ' || coalesce(u.last_name, '') as volunteer_name,
    j.school_id,
    j.academic_year,
    j.is_active,
    j.planned_sessions,
    j.attended_sessions,
    j.original_sessions,
    j.substitute_sessions,
    -- TODO: update multiplier when actual session duration per slot is available.
    j.original_sessions * 2 as hours_contributed,
    case
        when j.planned_sessions = 0 then null
        else round(j.original_sessions::numeric / nullif(j.planned_sessions, 0) * 100, 1)
    end as attendance_pct,
    case
        when j.planned_sessions = 0 then 'No Sessions Yet'
        when round(j.original_sessions::numeric / nullif(j.planned_sessions, 0) * 100, 1) >= 90 then 'Healthy'
        when round(j.original_sessions::numeric / nullif(j.planned_sessions, 0) * 100, 1) >= 75 then 'At Risk'
        else 'Unhealthy'
    end as consistency_status
from joined j
left join {{ ref('stg_pc_user') }} u
    on j.volunteer_id = u.user_id
order by j.school_id, consistency_status
