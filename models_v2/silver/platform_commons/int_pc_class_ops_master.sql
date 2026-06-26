{{ config(materialized='table') }}

with batch_attendance as (
    select * from {{ ref('stg_pc_batch_attendance') }}
),

level_batch as (
    select * from {{ ref('stg_pc_sc_level_batch') }}
),

sc_level_id_table as (
    select * from {{ ref('stg_pc_sc_level_id') }}
),

batch_name_bridge as (
    select 
        *,
        row_number() over (partition by sc_level_batch_id order by batch_name_id desc) as rn
    from {{ ref('stg_pc_scLevelBatch_batchName_bridge') }}
),

batch_name as (
    select * from {{ ref('stg_pc_batch_name') }}
),

level_ as (
    select * from {{ ref('stg_pc_level') }}
),

level_name_bridge as (
    select 
        *,
        row_number() over (partition by level_id order by level_name_id desc) as rn
    from {{ ref('stg_pc_level_levelName_bridge') }}
),

level_name as (
    select * from {{ ref('stg_pc_level_name') }}
),

school_course as (
    select * from {{ ref('stg_pc_school_course') }}
),

course as (
    select * from {{ ref('stg_pc_course') }}
),

field as (
    select * from {{ ref('stg_pc_field') }}
),

field_bridge as (
    select 
        *,
        row_number() over (partition by field_id order by field_name_id desc) as rn
    from {{ ref('stg_pc_field_field_name_bridge') }}
),

field_name as (
    select * from {{ ref('stg_pc_field_name') }}
),

course_name_bridge as (
    select 
        *,
        row_number() over (partition by course_id order by course_name_id desc) as rn
    from {{ ref('stg_pc_course_course_name_bridge') }}
),

course_name as (
    select * from {{ ref('stg_pc_course_name') }}
),

school as (
    select * from {{ ref('stg_pc_school') }}
),

school_name_bridge as (
    select 
        *,
        row_number() over (partition by school_id order by school_name_id desc) as rn
    from {{ ref('stg_pc_school_schoolName_bridge') }}
),

school_name as (
    select * from {{ ref('stg_pc_school_name') }}
),

university as (
    select * from {{ ref('stg_pc_university') }}
),

university_name_bridge as (
    select 
        *,
        row_number() over (partition by university_id order by university_name_id desc) as rn
    from {{ ref('stg_pc_university_universityName_bridge') }}
),

university_name as (
    select * from {{ ref('stg_pc_university_name') }}
),

slot_shift as (
    select * from {{ ref('stg_pc_worknode_slot_shift') }}
),

slot as (
    select * from {{ ref('stg_pc_worknode_slot') }}
),

slot_shift_list_bridge as (
    select * from {{ ref('stg_pc_worknode_slot_shift_list_bridge') }}
),

batch_slot_mapping as (
    select
        wss.for_entity_id as sc_level_batch_id,
        ws.day_of_week,
        wss.worknode_slot_shift_id,
        wss.supervisor_id,
        ws.worknode_slot_id,
        ws.slot_name,
        ws.start_time,
        ws.end_time
    from slot_shift wss
    join slot_shift_list_bridge sslb on wss.worknode_slot_shift_id = sslb.worknode_slot_shift_id
    join slot ws on sslb.worknode_slot_id = ws.worknode_slot_id
    where wss.for_entity_type = 'LTLD_SCLEVEL_BATCH'
),

substitute as (
    select * from {{ ref('stg_pc_substitute') }}
),

"user" as (
    select * from {{ ref('stg_pc_user') }}
),

batch_events as (
    select * from {{ ref('stg_pc_sc_level_batch_events') }}
),

batch_events_bridge as (
    select * from {{ ref('stg_pc_sc_level_batch_events_bridge') }}
),

-- Get the latest inactive event per batch
batch_inactive as (
    select
        beb.sc_level_batch_id,
        be.reason as inactive_reason,
        be.start_date as inactive_start_date,
        be.end_date as inactive_end_date,
        row_number() over (partition by beb.sc_level_batch_id order by be.start_date desc) as rn
    from batch_events_bridge beb
    join batch_events be on beb.batch_event_id = be.batch_event_id
),

center_city_mapping as (
    with center_city_workforce as (
        select 
            wf_center.worknode_id as center_id,
            w_city.worknode_name as city_name,
            row_number() over (partition by wf_center.worknode_id order by count(*) desc) as rn
        from {{ ref('stg_pc_workforce') }} wf_center
        join {{ ref('stg_pc_workforce') }} wf_city on wf_center.user_id = wf_city.user_id
        join {{ ref('stg_pc_worknode') }} w_center on wf_center.worknode_id = w_center.worknode_id and w_center.worknode_type = 'WN_TYPE.CENTER'
        join {{ ref('stg_pc_worknode') }} w_city on wf_city.worknode_id = w_city.worknode_id and w_city.worknode_type = 'WN_TYPE.CITY'
        group by 1, 2
    ),
    center_city_address as (
        select 
            wf.worknode_id as center_id,
            coalesce(c.city_name, a.city) as city_name,
            row_number() over (partition by wf.worknode_id order by count(*) desc) as rn
        from {{ ref('stg_pc_workforce') }} wf
        join {{ ref('stg_pc_worknode') }} w_center on wf.worknode_id = w_center.worknode_id and w_center.worknode_type = 'WN_TYPE.CENTER'
        join {{ ref('stg_pc_user') }} u on wf.user_id = u.user_id
        join {{ ref('stg_pc_person') }} p on u.person_id = p.person_id
        join {{ ref('stg_pc_person_person_addresses_bridge') }} pab on p.person_id = pab.person_id
        join {{ ref('stg_pc_person_addresses') }} pa on pab.person_address_id = pa.person_address_id
        join {{ ref('stg_pc_address') }} a on pa.address_id = a.address_id
        left join {{ ref('stg_pc_city') }} c on a.city = c.city_data_code and c.rn = '1'
        where pa.is_active = true
        group by 1, 2
    )
    select 
        w.worknode_name as center_name,
        coalesce(
            (select city_name from center_city_workforce ccw where ccw.center_id = w.worknode_id and ccw.rn = 1),
            (select city_name from center_city_address cca where cca.center_id = w.worknode_id and cca.rn = 1)
        ) as city_name
    from {{ ref('stg_pc_worknode') }} w
    where w.worknode_type = 'WN_TYPE.CENTER'
)

select
    -- Surrogate Keys
    {{ dbt_utils.generate_surrogate_key(['lb.sc_level_batch_id']) }} as class_key,
    {{ dbt_utils.generate_surrogate_key(['lb.sc_level_batch_id', 'ba.attendance_date']) }} as session_key,

    -- Identifiers
    ba.batch_attendance_id as "SessionId",
    lb.sc_level_batch_id as "BatchId",
    
    -- Hierarchy Details
    ln.class as "ClassName",
    cn.course_name as "CourseName",
    bn.section as "Section",
    bn.batch_name_id as "SectionId",
    fn.field_label as "Stream",
    case 
        when ba.is_active = true then 'Active' 
        else 'Inactive' 
    end as "batchStatus",
    
    -- Center Details
    coalesce(ccm.center_name, sn.center_name) as "CenterName",
    sch.school_id as "SchoolId",
    ccm.city_name as "CityName",
    sch.is_active as "CenterActiveStatus",
    
    -- Slot / Session Details
    coalesce(bsm_direct.worknode_slot_id, bsm_fallback.worknode_slot_id) as "CenterSlotId",
    u.user_id as "SlotMentorId",
    coalesce(bsm_direct.slot_name, bsm_fallback.slot_name) as "CenterSlotName",
    coalesce(bsm_direct.start_time, bsm_fallback.start_time) as "CenterSlotStartTime",
    coalesce(bsm_direct.end_time, bsm_fallback.end_time) as "CenterSlotEndTime",
    coalesce(bsm_direct.day_of_week, bsm_fallback.day_of_week) as "CenterSlotDayOfWeek",
    coalesce(ba.for_slot_shift_id, bsm_fallback.worknode_slot_shift_id) as "SectionSlotShiftId",
    
    -- Substitution Details
    sub.substitute_id as "SubstituteId",
    sub.for_user_id as "SubstitutedUserId",
    sub.request_type as "RequestType",
    sub.requesting_reason as "Reason",
    
    -- Session Metadata
    ba.subject_code as "SubjectCode",
    ba.attendance_date as "ScheduledSessionDate",
    ba.captured_by_user_id as "AttendanceTakenByUserId",
    ba.is_active as "SessionActiveStatus",

    -- New columns from gap analysis
    lb.academic_year as "AcademicYear",
    u.first_name || ' ' || coalesce(u.last_name, '') as "VolunteerAssigned",
    bi.inactive_reason as "BatchInactiveReason",
    bi.inactive_start_date as "BatchInactiveStartDate",
    bi.inactive_end_date as "BatchInactiveEndDate",
    lb.updated_datetime as "UserUpdatedDateTime",
    un.university_name as "UniversityName"

from batch_attendance ba
left join level_batch lb on ba.sc_level_batch_id = lb.sc_level_batch_id
left join batch_name_bridge bnb on lb.sc_level_batch_id = bnb.sc_level_batch_id and bnb.rn = 1
left join batch_name bn on bnb.batch_name_id = bn.batch_name_id

-- Join path for Level/Course
left join sc_level_id_table slit on lb.sc_level_id = slit.sc_level_id_table_id
left join level_ l on slit.level_id = l.level_id
left join level_name_bridge lnb on l.level_id = lnb.level_id and lnb.rn = 1
left join level_name ln on lnb.level_name_id = ln.level_name_id and ln.language_code = 'ENG'

left join school_course sc on slit.school_course_id = sc.school_course_id
left join course c on sc.course_id = c.course_id
left join course_name_bridge cnb on c.course_id = cnb.course_id and cnb.rn = 1
left join course_name cn on cnb.course_name_id = cn.course_name_id and cn.language_code = 'ENG'

left join field f on c.field_id = f.field_id
left join field_bridge fbr on f.field_id = fbr.field_id and fbr.rn = 1
left join field_name fn on fbr.field_name_id = fn.field_name_id and fn.language_code = 'ENG'

left join school sch on sc.school_id = sch.school_id
left join school_name_bridge snb on sch.school_id = snb.school_id and snb.rn = 1
left join school_name sn on snb.school_name_id = sn.school_name_id and sn.language_code = 'ENG'

left join university univ on sch.university_id = univ.university_id
left join university_name_bridge unb on univ.university_id = unb.university_id and unb.rn = 1
left join university_name un on unb.university_name_id = un.university_name_id and (un.language_code = 'ENG' or un.language_code = '' or un.language_code is null)

left join batch_slot_mapping bsm_direct on ba.for_slot_shift_id = bsm_direct.worknode_slot_shift_id
left join batch_slot_mapping bsm_fallback on ba.sc_level_batch_id = bsm_fallback.sc_level_batch_id 
    and ba.for_slot_shift_id is null
    and bsm_fallback.day_of_week = to_char(ba.attendance_date::date, 'FMDay')
left join "user" u on coalesce(bsm_direct.supervisor_id, bsm_fallback.supervisor_id) = u.user_id
left join substitute sub on 
    (ba.for_slot_shift_id = sub.for_slot_shift_id or (ba.for_slot_shift_id is null and bsm_fallback.worknode_slot_shift_id = sub.for_slot_shift_id))
    and ba.attendance_date::date = sub.for_date::date
left join batch_inactive bi on lb.sc_level_batch_id = bi.sc_level_batch_id and bi.rn = 1
left join center_city_mapping ccm on lower(trim(sn.center_name)) = lower(trim(ccm.center_name))
