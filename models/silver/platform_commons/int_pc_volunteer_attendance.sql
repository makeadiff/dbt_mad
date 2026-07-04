{{ config(materialized='table') }}

with attendance as (
    select * from {{ ref('stg_pc_volunteer_attendance') }}
    -- DB stores full codes e.g. ATTENDANCE_STATUS.PRESENT; CSV shows plain PRESENT/ABSENT
    where attendance_status in ('ATTENDANCE_STATUS.PRESENT', 'ATTENDANCE_STATUS.ABSENT')
),

substitute as (
    select * from {{ ref('stg_pc_substitute') }}
),

-- Session-level context (class, center, city, slot, mentor) from class ops master.
-- Deduped to one row per slot_shift + date; prioritise rows where CenterName is populated.
session_context as (
    select distinct on ("SectionSlotShiftId", "ScheduledSessionDate"::date)
        "SectionSlotShiftId"::bigint                    as section_slot_shift_id,
        "ScheduledSessionDate"::date                    as scheduled_session_date,
        "Stream"                                        as stream,
        "CourseName"                                    as course,
        "CityName"                                      as city_name,
        "CenterName"                                    as center_name,
        "SchoolId"                                      as school_id,
        "CenterSlotId"                                  as center_slot_id,
        "CenterSlotName"                                as center_slot_name,
        "CenterSlotDayOfWeek"                           as center_slot_day_of_week,
        "CenterSlotStartTime"                           as center_slot_start_time,
        "CenterSlotEndTime"                             as center_slot_end_time,
        "ClassName"                                     as class_name,
        "Section"                                       as section,
        "SectionId"                                     as section_id,
        "SlotMentorId"                                  as slot_mentor_id,
        "VolunteerAssigned"                             as slot_mentor_name,
        "SessionId"                                     as class_id,
        "AttendanceTakenByUserId"                       as class_attendance_taken_by_user_id,
        "CenterActiveStatus"                            as center_active_status
    from {{ ref('int_pc_class_ops_master') }}
    -- Rows with CenterName take priority so COALESCE below finds them first
    order by "SectionSlotShiftId", "ScheduledSessionDate"::date, "CenterName" nulls last
),

center_city_mapping as (
    select 
        w_center.worknode_name as center_name,
        w_city.worknode_name as city_name
    from {{ ref('stg_pc_worknode') }} w_center
    join {{ ref('stg_pc_worknode_hierarchy') }} wh 
        on w_center.worknode_id = wh.worknode_id
    join {{ ref('stg_pc_worknode') }} w_city 
        on wh.parent_worknode_id = w_city.worknode_id
    where w_center.worknode_type = 'WN_TYPE.CENTER'
      and w_city.worknode_type = 'WN_TYPE.CITY'
      and wh.depth = 1
      and wh.is_active = true
),

-- CENTER name via worknodeSlotShift → batch → school → school_name
-- CITY name via Center -> worknodeHierarchy -> City
-- One row per slot_shift_id (for_slot_shift_id in attendance)
slot_center_city as (
    select distinct on (wss.worknode_slot_shift_id)
        wss.worknode_slot_shift_id      as slot_shift_id,
        wss.supervisor_id               as supervisor_id,
        sn.center_name                  as center_name,
        ccm.city_name                   as city_name,
        sch.is_active                   as center_active_status
    from {{ ref('stg_pc_worknode_slot_shift') }} wss
    -- batch → sc_level_id → school_course → school
    left join {{ ref('stg_pc_sc_level_batch') }} lb
        on wss.for_entity_id = lb.sc_level_batch_id
    left join {{ ref('stg_pc_sc_level_id') }} slit
        on lb.sc_level_id = slit.sc_level_id_table_id
    left join {{ ref('stg_pc_school_course') }} sc
        on slit.school_course_id = sc.school_course_id
    left join {{ ref('stg_pc_school') }} sch
        on sc.school_id = sch.school_id
    -- school → school_name (= center_name)
    left join {{ ref('stg_pc_school_schoolName_bridge') }} snb
        on sc.school_id = snb.school_id
    left join {{ ref('stg_pc_school_name') }} sn
        on snb.school_name_id = sn.school_name_id
    left join center_city_mapping ccm
        on lower(trim(sn.center_name)) = lower(trim(ccm.center_name))
    where wss.for_entity_type = 'LTLD_SCLEVEL_BATCH'
      and (wss.is_deleted is false or wss.is_deleted is null)
    order by wss.worknode_slot_shift_id, sn.center_name nulls last, ccm.city_name nulls last
),

-- User lookup — reused multiple times as aliases below
user_names as (
    select
        user_id,
        trim(first_name || ' ' || coalesce(last_name, '')) as full_name
    from {{ ref('stg_pc_user') }}
),

-- Maps community_member_id → user_id via:
--   communityMemberAttendance.communityMemberId
--   → communityMember.id (actorId points to actor table)
--   → actor.id → actor.actorId (the actual user_id)
vol_user_map as (
    select
        cm.id        as community_member_id,
        a."actorId"  as user_id
    from pc_raw."communityMember" cm
    join pc_raw."actor" a on cm."actorId" = a.id
    where a."actorTypeDataCode" = 'ACTOR_TYPE.USER'
      and (cm."xIsDeleted" is false or cm."xIsDeleted" is null)
)

select distinct on (a.attendance_id)
    -- Surrogate keys (kept from original model)
    {{ dbt_utils.generate_surrogate_key(['a.attendance_id']) }}                         as volunteer_attendance_key,
    {{ dbt_utils.generate_surrogate_key(['a.for_slot_shift_id', 'a.attendance_date']) }} as session_key,
    {{ dbt_utils.generate_surrogate_key(['a.community_member_id']) }}                   as volunteer_key,

    -- ── Session Context ──────────────────────────────────────────────────────────
    sc.stream,
    sc.course,
    -- city_name: class_ops value first, then direct slot→supervisor→workforce→city lookup
    coalesce(sc.city_name, scc.city_name)                                               as city_name,
    -- center_name: class_ops value first, then direct slot→batch→school→school_name lookup
    coalesce(sc.center_name, scc.center_name)                                           as center_name,
    coalesce(sc.center_active_status, scc.center_active_status)                         as center_active_status,
    sc.school_id,
    sc.center_slot_id,
    sc.center_slot_name,
    sc.center_slot_day_of_week,
    sc.center_slot_start_time,
    sc.center_slot_end_time,
    sc.class_name,
    sc.section,
    sc.section_id,
    a.for_slot_shift_id                                                                 as section_slot_shift_id,
    sc.slot_mentor_id,
    sc.slot_mentor_name,
    a.subject_code,
    a.attendance_date                                                                    as scheduled_session_date,

    -- ── Tagged Volunteer ─────────────────────────────────────────────────────────
    a.attendance_id,
    a.community_member_id                                                               as tagged_volunteer_id,
    vol.full_name                                                                       as tagged_volunteer_name,
    -- isAttendanceTakenForTaggedVolunteer: TRUE for all rows (already filtered to PRESENT/ABSENT)
    true                                                                                as is_attendance_taken_for_tagged_volunteer,
    vol.full_name                                                                       as volunteer_name,
    -- Normalized to match CSV format (strip ATTENDANCE_STATUS. prefix)
    {{ clean_prefix('a.attendance_status') }}                                           as attendance_status,
    {{ clean_prefix('a.zero_attendance_status') }}                                      as zero_attendance_status,

    -- ── Substitution Info (sourced from worknodeSlotShiftSubstitute via stg_pc_substitute) ────
    -- byUser  = assignee (who covers the slot)
    -- forUser = substituted volunteer (who is being replaced; NULL for CANCELLATION type)
    assignee_user.full_name                                                             as assignee_user_name,
    s.substitute_id,
    s.by_user_id                                                                        as assignee_user_id,
    s.for_user_id                                                                       as substituted_volunteer_user_id,
    subst_vol.full_name                                                                 as substituted_volunteer_user_name,
    {{ clean_prefix('s.request_status') }}                                              as request_status,
    {{ clean_prefix('s.request_type') }}                                                as substitution_type,
    {{ clean_prefix('s.requesting_reason') }}                                           as substitution_reason,

    -- ── Class Attendance Metadata ────────────────────────────────────────────────
    sc.class_attendance_taken_by_user_id,
    att_taker.full_name                                                                 as class_attendance_taken_by_user_name,
    case
        when s.substitute_id is not null and {{ clean_prefix('s.request_type') }} <> 'CANCELLATION' then
            case
                when {{ clean_prefix('s.request_type') }} = 'SUBSTITUTE' and sc.class_attendance_taken_by_user_id is not null then 'PRESENT'
                else 'ABSENT'
            end
        else null
    end                                                                                 as substitute_volunteer_attendance_status,

    -- ── Identifiers ─────────────────────────────────────────────────────────────
    sc.class_id

from attendance a

-- Session context: many volunteers → one session row
left join session_context sc
    on  a.for_slot_shift_id     = sc.section_slot_shift_id
    and (a.attendance_date::date = sc.scheduled_session_date::date or a.attendance_date::date = (sc.scheduled_session_date::date - interval '1 day')::date)

-- Substitution for this slot + date
left join substitute s
    on  a.for_slot_shift_id     = s.for_slot_shift_id
    and (a.attendance_date::date = s.for_date::date or a.attendance_date::date = (s.for_date::date - interval '1 day')::date)

-- Volunteer community_member_id → user_id resolution
left join vol_user_map vm
    on a.community_member_id = vm.community_member_id

-- Volunteer name (via resolved user_id)
left join user_names vol
    on vm.user_id = vol.user_id

-- Assignee (person who will cover the slot) name
left join user_names assignee_user
    on s.by_user_id = assignee_user.user_id

-- Substituted volunteer (person being replaced) name
left join user_names subst_vol
    on s.for_user_id = subst_vol.user_id

-- Class attendance taker name
left join user_names att_taker
    on sc.class_attendance_taken_by_user_id::bigint = att_taker.user_id

-- CENTER + CITY via worknodeSlotShift → school (center) and supervisor → workforce (city)
left join slot_center_city scc
    on a.for_slot_shift_id = scc.slot_shift_id
order by a.attendance_id, abs(a.attendance_date::date - sc.scheduled_session_date::date) asc nulls last
