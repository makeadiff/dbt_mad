{{ config(
    materialized='table'
) }}

with attendance_header as (
    select * from {{ ref('stg_pc_batch_attendance') }}
),

batch_student as (
    select * from {{ ref('stg_pc_batch_student') }}
),

student_attendance as (
    select * from {{ ref('stg_pc_batch_student_attendance') }}
),

student as (
    select * from {{ ref('stg_pc_student') }}
),

ie as (
    select * from {{ ref('stg_pc_ie') }}
),

"user" as (
    select * from {{ ref('stg_pc_user') }}
),

profile as (
    select * from {{ ref('stg_pc_person_profile') }}
),

person as (
    select * from {{ ref('stg_pc_person') }}
),

gender as (
    select * from {{ ref('stg_pc_gender') }} where rn = '1'
),

level_batch as (
    select * from {{ ref('stg_pc_sc_level_batch') }}
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

sc_level_id_table as (
    select * from {{ ref('stg_pc_sc_level_id') }}
),

level_table as (
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

address as (
    select * from {{ ref('stg_pc_address') }}
),

slot_shift as (
    select * from {{ ref('stg_pc_worknode_slot_shift') }}
),

slot_shift_list_bridge as (
    select * from {{ ref('stg_pc_worknode_slot_shift_list_bridge') }}
),

slot as (
    select * from {{ ref('stg_pc_worknode_slot') }}
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

feedback_bridge as (
    select * from {{ ref('stg_pc_bsa_feedback_responses_bridge') }}
),

feedback_responses as (
    select * from {{ ref('stg_pc_feedback_responses') }}
),

fr_dro_bridge as (
    select * from {{ ref('stg_pc_fr_dro_bridge') }}
),

default_response_options as (
    select * from {{ ref('stg_pc_default_response_options') }}
),

options as (
    select * from {{ ref('stg_pc_options') }}
),

options_name_bridge as (
    select * from {{ ref('stg_pc_options_optionName_bridge') }}
),

option_name as (
    select * from {{ ref('stg_pc_option_name') }}
),

-- Aggregate tagged volunteer IDs per slot shift
tagged_volunteers as (
    select
        wssub.worknode_slot_shift_id,
        string_agg(ssul.owner_user_id, ',') as tagged_volunteer_ids
    from {{ ref('stg_pc_worknode_slot_shift_user_bridge') }} wssub
    join {{ ref('stg_pc_worknode_slot_shift_user_list') }} ssul 
        on wssub.slot_shift_user_id = ssul.slot_shift_user_id
    group by 1
),

-- Aggregate feedback responses per attendance record
aggregated_feedback as (
    select
        fb.batch_student_attendance_id,
        jsonb_object_agg(
            fr.question_id::text,
            coalesce(onm.option_label, fr.response_value)
        ) as feedback_json
    from feedback_bridge fb
    join feedback_responses fr on fb.feedback_response_id = fr.feedback_response_id
    left join fr_dro_bridge frb on fr.feedback_response_id = frb.feedback_response_id
    left join default_response_options dro on frb.default_response_option_id = dro.default_response_option_id
    left join options opt on dro.options_id = opt.options_id
    left join options_name_bridge onm_br on opt.options_id = onm_br.options_id
    left join option_name onm on onm_br.option_name_id = onm.option_name_id and onm.language_code = 'ENG'
    group by 1
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
)

select distinct on (
    bs.student_id,
    ba.attendance_date,
    ba.batch_attendance_id
)
    -- Surrogate Keys
    {{ dbt_utils.generate_surrogate_key(['bs.student_id', 'ba.attendance_date', 'ba.batch_attendance_id']) }} as attendance_key,
    {{ dbt_utils.generate_surrogate_key(['bs.student_id']) }} as student_key,
    {{ dbt_utils.generate_surrogate_key(['lb.sc_level_batch_id']) }} as class_key,
    {{ dbt_utils.generate_surrogate_key(['lb.sc_level_batch_id', 'ba.attendance_date']) }} as session_key,

    -- Identifiers
    bs.student_id as "ChildId",
    bs.is_active as "ChildActiveStatus",
    p.first_name || ' ' || coalesce(p.last_name, '') as "ChildName",
    upper(coalesce(p.gender_identifier, g.gender_label)) as "Gender",
    
    -- Academic Details
    ln.class as "Class",
    cn.course_name as "Course",
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
    {{ clean_prefix('sub.request_type') }} as "RequestType",
    {{ clean_prefix('sub.requesting_reason') }} as "Reason",
    
    -- Attendance Status
    {{ clean_prefix('bsa.attendance_status') }} as "ChildAttendanceStatus",
    ba.subject_code as "SubjectCode",
    ba.attendance_date as "ScheduledSessionDate",
    ba.captured_by_user_id as "AttendanceTakenByUserId",
    
    -- Feedback
    af.feedback_json,
    nullif(af.feedback_json ->> '{{ var("pc_question_did_actively_participate", "34237") }}', '')::integer as did_actively_participate,
    nullif(af.feedback_json ->> '{{ var("pc_question_did_understand_concepts", "34238") }}', '')::integer as did_understand_concepts,
    nullif(af.feedback_json ->> '{{ var("pc_question_did_complete_assigned_task", "34239") }}', '')::integer as did_complete_assigned_task,
    nullif(af.feedback_json ->> '{{ var("pc_question_additional_notes", "34240") }}', '')::text as additional_notes,
    nullif(af.feedback_json ->> '{{ var("pc_question_reason_for_child_absence", "34244") }}', '')::text as reason_for_the_childs_absence,
    
    -- New columns from gap analysis
    ba.created_datetime as "SlotCreatedDateTime",
    u.updated_datetime as "UserUpdatedDateTime",
    tv.tagged_volunteer_ids as "TaggedVolunteerIds"

from attendance_header ba
join batch_student bs on ba.sc_level_batch_id = bs.sc_level_batch_id
left join student_attendance bsa on bsa.batch_student_id = bs.batch_student_id 
    and bsa.ba_uid = ba.alias_id
left join student s on bs.student_id = s.student_id
left join ie i on s.ie_id = i.ie_id
left join person pr on i.person_id = pr.person_id
left join profile p on pr.person_profile_id = p.person_profile_id
left join gender g on p.gender_data_code = g.gender_data_code
left join level_batch lb on ba.sc_level_batch_id = lb.sc_level_batch_id
left join batch_name_bridge bnb on lb.sc_level_batch_id = bnb.sc_level_batch_id and bnb.rn = 1
left join batch_name bn on bnb.batch_name_id = bn.batch_name_id

-- Join path for Slot / Session
left join batch_slot_mapping bsm_direct on ba.for_slot_shift_id = bsm_direct.worknode_slot_shift_id
left join batch_slot_mapping bsm_fallback on ba.sc_level_batch_id = bsm_fallback.sc_level_batch_id 
    and ba.for_slot_shift_id is null
    and bsm_fallback.day_of_week = to_char(ba.attendance_date::date, 'FMDay')
left join "user" u on coalesce(bsm_direct.supervisor_id, bsm_fallback.supervisor_id) = u.user_id
left join substitute sub on 
    (ba.for_slot_shift_id = sub.for_slot_shift_id or (ba.for_slot_shift_id is null and bsm_fallback.worknode_slot_shift_id = sub.for_slot_shift_id))
    and ba.attendance_date::date = sub.for_date::date

-- Join path for Class
left join sc_level_id_table slit on lb.sc_level_id = slit.sc_level_id_table_id
left join level_table lt on slit.level_id = lt.level_id
left join level_name_bridge lnb on lt.level_id = lnb.level_id and lnb.rn = 1
left join level_name ln on lnb.level_name_id = ln.level_name_id and ln.language_code = 'ENG'

-- Join path for School/Center
left join school_course sc on slit.school_course_id = sc.school_course_id
left join school sch on sc.school_id = sch.school_id
left join school_name_bridge snb on sch.school_id = snb.school_id and snb.rn = 1
left join school_name sn on snb.school_name_id = sn.school_name_id and sn.language_code = 'ENG'
left join university univ on sch.university_id = univ.university_id
left join university_name_bridge unb on univ.university_id = unb.university_id and unb.rn = 1
left join university_name un on unb.university_name_id = un.university_name_id and (un.language_code = 'ENG' or un.language_code = '' or un.language_code is null)
-- left join address addr on sch.address_id = addr.address_id

-- Join path for Course
left join course c on sc.course_id = c.course_id
left join course_name_bridge cnb on c.course_id = cnb.course_id and cnb.rn = 1
left join course_name cn on cnb.course_name_id = cn.course_name_id and cn.language_code = 'ENG'

-- Join path for Stream
left join field f on c.field_id = f.field_id
left join field_bridge fbr on f.field_id = fbr.field_id and fbr.rn = 1
left join field_name fn on fbr.field_name_id = fn.field_name_id

left join aggregated_feedback af on bsa.batch_student_attendance_id = af.batch_student_attendance_id
left join tagged_volunteers tv on coalesce(bsm_direct.worknode_slot_shift_id, bsm_fallback.worknode_slot_shift_id) = tv.worknode_slot_shift_id
left join center_city_mapping ccm on lower(trim(sn.center_name)) = lower(trim(ccm.center_name))

where ba.is_deleted = false
order by
    bs.student_id,
    ba.attendance_date,
    ba.batch_attendance_id,
    bsa.batch_student_attendance_id desc nulls last
