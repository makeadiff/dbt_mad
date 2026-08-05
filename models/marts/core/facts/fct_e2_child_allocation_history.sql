{{ config(materialized='table') }}

-- fct_e2_child_allocation_history: child-to-slot assignment history with class/subject/session context
-- Grain: one row per (child_id, slot_class_section_id) allocation
-- Ported from the legacy child_allocation_history_e2_sessions model. Volunteer columns are
-- intentionally excluded so child coverage stays countable even when a section currently has
-- no volunteer assigned (see fct_e2_child_consistency, which relies on this independence).

select
    ch.child_id,
    ch.first_name as child_first_name,
    ch.last_name as child_last_name,
    scs.slot_class_section_id,
    cs.section_name,
    c.class_name,
    sub.subject_name,
    p.partner_id,
    p.partner_name,
    s.day_of_week,
    ay.label as academic_year,
    coalesce(
        ssd.start_date,
        case say.academic_year_id
            when 1 then '2025-07-15'::date
            when 2 then '2026-07-15'::date
            else null
        end
    ) as sessions_start_date
from {{ ref('int_bubble__child_class_section') }} ccs
join {{ ref('int_bubble__children') }} ch
    on ccs.child_id = ch.child_id
join {{ ref('int_bubble__class_section') }} cs
    on ccs.class_section_id = cs.class_section_id
join {{ ref('int_bubble__slot_class_section') }} scs
    on cs.class_section_id = scs.class_section_id
join {{ ref('int_bubble__slot') }} s
    on scs.slot_id = s.slot_id
join {{ ref('int_bubble__school_class') }} sc
    on cs.school_class_id = sc.school_class_id
join {{ ref('int_bubble__class') }} c
    on sc.class_id = c.class_id
join {{ ref('int_bubble__class_section_subject') }} css
    on scs.class_section_subject_id = css.class_section_subject_id
join {{ ref('int_bubble__subject') }} sub
    on css.subject_id = sub.subject_id
    and sub.is_removed = false
left join {{ ref('int_bubble__school_academic_year') }} say
    on s.school_academic_year_id = say.school_academic_year_id
left join {{ ref('int_bubble__academic_year') }} ay
    on say.academic_year_id = ay.academic_year_id
left join {{ ref('int_bubble__school_session_detail') }} ssd
    on say.school_academic_year_id = ssd.school_academic_year_id
    and cs.school_id = ssd.school_id
join {{ ref('int_bubble__partner') }} p
    on cs.school_id = p.partner_id
where
    scs.is_removed = false
    and cs.is_removed = false
    and p.is_removed = false
    and ccs.is_removed = false
