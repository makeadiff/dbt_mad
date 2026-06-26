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

substitute as (
    select * from {{ ref('stg_pc_substitute') }}
),

"user" as (
    select * from {{ ref('stg_pc_user') }}
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
    sn.center_name as "CenterName",
    sch.school_id as "SchoolId",
    un.city_name as "CityName",
    
    -- Slot / Session Details
    wss.worknode_slot_id as "CenterSlotId",
    u.user_id as "SlotMentorId",
    ws.slot_name as "CenterSlotName",
    ws.start_time as "CenterSlotStartTime",
    ws.end_time as "CenterSlotEndTime",
    ws.day_of_week as "CenterSlotDayOfWeek",
    ba.for_slot_shift_id as "SectionSlotShiftId",
    
    -- Substitution Details
    sub.substitute_id as "SubstituteId",
    sub.for_user_id as "SubstitutedUserId",
    sub.request_type as "RequestType",
    sub.requesting_reason as "Reason",
    
    -- Session Metadata
    ba.subject_code as "SubjectCode",
    ba.attendance_date as "ScheduledSessionDate",
    ba.captured_by_user_id as "AttendanceTakenByUserId",
    ba.is_active as "SessionActiveStatus"

from batch_attendance ba
left join level_batch lb on ba.sc_level_batch_id = lb.sc_level_batch_id
left join "user" u on lb.trainer_id = u.user_id
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

left join school sch on lb.tenant_id = sch.school_id
left join school_name_bridge snb on sch.school_id = snb.school_id and snb.rn = 1
left join school_name sn on snb.school_name_id = sn.school_name_id and sn.language_code = 'ENG'

left join university univ on sch.university_id = univ.university_id
left join university_name_bridge unb on univ.university_id = unb.university_id and unb.rn = 1
left join university_name un on unb.university_name_id = un.university_name_id and un.language_code = 'ENG'

left join slot_shift wss on ba.for_slot_shift_id = wss.worknode_slot_shift_id
left join slot ws on wss.worknode_slot_id = ws.worknode_slot_id
left join slot_shift wss_alt on ba.sc_level_batch_id = wss_alt.for_entity_id and wss_alt.for_entity_type = 'SC_LEVEL_BATCH'
left join substitute sub on 
    (ba.for_slot_shift_id = sub.for_slot_shift_id or (ba.for_slot_shift_id is null and wss_alt.worknode_slot_shift_id = sub.for_slot_shift_id))
    and ba.attendance_date::date = sub.for_date::date
