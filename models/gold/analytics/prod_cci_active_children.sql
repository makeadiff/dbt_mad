{{ config(materialized='table') }}

-- CCI dashboard (E1): every active child per section for AY 2026-27, with the section's programme.
-- Shared base for prod_cci_mentoring_circles and prod_cci_chapter_summary, so the active-child
-- rule lives in one place. Not read by Dalgo.
--
-- Reaches into bronze (stg_pc_*) because no mart carries section grain with the active flags
-- (spec E1 §3.0). Built alongside, not on, the old prod_e1_* models (spec §6).
--
-- Chapter: a Platform Commons school with a section (scLevelBatch) in ACADEMIC_SESSION.2026-2027,
-- excluding Test School (29306).
-- Active child: student record active, and enrolment (batchStudent) active in such a section.
-- Programme (section's course): Foundation Program -> Foundations; Ed Support / Education Support
-- -> Education Support; Commerce / Humanities / Science -> Transition Readiness; no course ->
-- Programme not set. A child in two sections counts once per section.
--
-- Grain: one row per (student_id, sc_level_batch_id).

with sections as (
    select
        lb.sc_level_batch_id,
        sc.school_id::text as chapter_id,
        case cn.course_name
            when 'Foundation Program' then 'Foundations'
            when 'Ed Support' then 'Education Support'
            when 'Education Support' then 'Education Support'
            when 'Commerce' then 'Transition Readiness'
            when 'Humanities' then 'Transition Readiness'
            when 'Science' then 'Transition Readiness'
            else 'Programme not set'
        end as programme
    from {{ ref('stg_pc_sc_level_batch') }} lb
    join {{ ref('stg_pc_sc_level_id') }} li
        on lb.sc_level_id = li.sc_level_id_table_id
    join {{ ref('stg_pc_school_course') }} sc
        on li.school_course_id = sc.school_course_id
    left join {{ ref('stg_pc_course_course_name_bridge') }} b
        on sc.course_id = b.course_id
    left join {{ ref('stg_pc_course_name') }} cn
        on b.course_name_id = cn.course_name_id
    where lb.academic_year = 'ACADEMIC_SESSION.2026-2027'
      and sc.school_id <> 29306
)

select distinct
    s.chapter_id,
    s.sc_level_batch_id,
    bs.student_id,
    s.programme
from sections s
join {{ ref('stg_pc_batch_student') }} bs
    on s.sc_level_batch_id = bs.sc_level_batch_id
join {{ ref('stg_pc_student') }} st
    on bs.student_id = st.student_id
where bs.is_active
  and st.is_active
