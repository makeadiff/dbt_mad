{{ config(materialized='table') }}

-- E1 session-ops: resolves which batch each child is enrolled in, whether that batch is
-- currently active, and the school/center it belongs to (with the center's own active status).
-- Flow: stg_pc_batch_student + stg_pc_sc_level_batch + stg_pc_sc_level_id + stg_pc_school_course
--       + int_pc_school_id + stg_pc_level(_name) + stg_pc_course(_name) + stg_pc_student/ie/person/
--       person_profile/gender -> int_pc_child_batch_enrollment
-- Distinct from int_pc_child_attendance (attendance-event grain, and its "batchStatus" comes
-- from the attendance record's own is_active flag, not the batch entity's). Here:
--   - batch_active_status comes from sc_level_batch.is_active (the batch itself)
--   - enrollment_active_status comes from batch_student.is_active (whether THIS child's
--     membership in THIS batch is active -- a child can be inactive in an otherwise active batch)
--   - center_active_status comes from school.is_active (via int_pc_school_id, which already
--     resolves school_name through the bridge/language-code fallback)
-- Grain: one row per batch_student_id (a child's enrollment link to one batch)

with batch_student as (
    select * from {{ ref('stg_pc_batch_student') }}
),

level_batch as (
    select * from {{ ref('stg_pc_sc_level_batch') }}
),

sc_level_id_table as (
    select * from {{ ref('stg_pc_sc_level_id') }}
),

school_course as (
    select * from {{ ref('stg_pc_school_course') }}
),

school as (
    select * from {{ ref('int_pc_school_id') }}
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

student as (
    select * from {{ ref('stg_pc_student') }}
),

ie as (
    select * from {{ ref('stg_pc_ie') }}
),

person as (
    select * from {{ ref('stg_pc_person') }}
),

profile as (
    select * from {{ ref('stg_pc_person_profile') }}
),

gender as (
    select * from {{ ref('stg_pc_gender') }} where rn = '1'
)

select
    bs.batch_student_id,
    bs.student_id,
    p.first_name || ' ' || coalesce(p.last_name, '') as student_name,
    upper(coalesce(p.gender_identifier, g.gender_label)) as gender,
    bs.sc_level_batch_id as batch_id,
    {{ clean_prefix('lb.academic_year') }} as academic_year,
    lb.is_active as batch_active_status,
    bs.batch_student_status,
    bs.is_active as enrollment_active_status,
    ln.class,
    cn.course_name as course,
    school.school_id,
    school.school_name,
    school.is_active as center_active_status,
    bs.created_datetime,
    bs.updated_datetime
from batch_student bs
left join level_batch lb on bs.sc_level_batch_id = lb.sc_level_batch_id
left join sc_level_id_table slit on lb.sc_level_id = slit.sc_level_id_table_id
left join school_course sc on slit.school_course_id = sc.school_course_id
left join school on sc.school_id = school.school_id
left join level_table lt on slit.level_id = lt.level_id
left join level_name_bridge lnb on lt.level_id = lnb.level_id and lnb.rn = 1
left join level_name ln on lnb.level_name_id = ln.level_name_id and ln.language_code = 'ENG'
left join course c on sc.course_id = c.course_id
left join course_name_bridge cnb on c.course_id = cnb.course_id and cnb.rn = 1
left join course_name cn on cnb.course_name_id = cn.course_name_id and cn.language_code = 'ENG'
left join student s on bs.student_id = s.student_id
left join ie i on s.ie_id = i.ie_id
left join person pr on i.person_id = pr.person_id
left join profile p on pr.person_profile_id = p.person_profile_id
left join gender g on p.gender_data_code = g.gender_data_code
