{{ config(materialized='table') }}

with student as (
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
),

school_course as (
    select * from {{ ref('stg_pc_school_course') }}
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

sc_level_id as (
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
)

select
    s.student_id,
    upper(coalesce(p.gender_identifier, g.gender_label)) as gender,
    sc.school_id,
    sn.center_name as school,
    ln.class as class,
    cn.course_name as course,
    p.first_name || ' ' || coalesce(p.last_name, '') as student_name,
    s.is_active as active_status,
    s.student_code as student_role_no,
    s.medium_of_instruction,
    null::text as location,
    s.updated_datetime as user_updated_date_time

from student s
left join ie i on s.ie_id = i.ie_id
left join person pr on i.person_id = pr.person_id
left join profile p on pr.person_profile_id = p.person_profile_id
left join gender g on p.gender_data_code = g.gender_data_code
left join school_course sc on s.school_course_id = sc.school_course_id
left join sc_level_id sl on sc.school_course_id = sl.school_course_id
left join level_table lt on sl.level_id = lt.level_id
left join level_name_bridge lnb on lt.level_id = lnb.level_id and lnb.rn = 1
left join level_name ln on lnb.level_name_id = ln.level_name_id and ln.language_code = 'ENG'
left join school sch on sc.school_id = sch.school_id
left join school_name_bridge snb on sch.school_id = snb.school_id and snb.rn = 1
left join school_name sn on snb.school_name_id = sn.school_name_id and sn.language_code = 'ENG'
left join course c on sc.course_id = c.course_id
left join course_name_bridge cnb on c.course_id = cnb.course_id and cnb.rn = 1
left join course_name cn on cnb.course_name_id = cn.course_name_id and cn.language_code = 'ENG'
left join university univ on sch.university_id = univ.university_id
left join university_name_bridge unvb on univ.university_id = unvb.university_id and unvb.rn = 1
left join university_name un on unvb.university_name_id = un.university_name_id and un.language_code = 'ENG'
