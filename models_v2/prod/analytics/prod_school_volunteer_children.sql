{{ config(materialized='table') }}

-- School Volunteer Children Summary: Per-school metrics for MOU schools

with schools_with_mou as (
  -- TODO: Integrate Platform Commons (PC) data for schools and MOUs
  select distinct m.partner_id as school_id, p.partner_name as school_name
  from {{ ref('dim_mou') }} m
  inner join {{ ref('dim_crm_partner') }} p on m.partner_id = p.crm_partner_id
  where m.mou_status = 'active' and p.is_removed = false
),

school_slots as (
  -- TODO: Integrate Platform Commons (PC) data for teaching slots
  select distinct s.school_id, s.school_name, sl.slot_id
  from schools_with_mou s
  left join {{ ref('dim_slot') }} sl on s.school_id = sl.school_id
),

class_section_volunteers as (
  -- TODO: Integrate Platform Commons (PC) data for volunteer assignments
  select ss.school_id, ss.school_name,
    case when scsv.volunteer_id is not null then 1 else 0 end as has_volunteer
  from school_slots ss
  left join {{ ref('int_bubble__slot_class_section') }} scs on ss.slot_id = scs.slot_id and scs.is_removed = false
  left join {{ ref('int_bubble__slot_class_section_volunteer') }} scsv on scs.slot_class_section_id = scsv.slot_class_section_id and scsv.is_removed = false
),

school_volunteer_counts as (
  select school_id, school_name,
    count(case when has_volunteer = 1 then 1 end) as classes_with_volunteers,
    count(case when has_volunteer = 0 then 1 end) as classes_without_volunteers
  from class_section_volunteers
  group by school_id, school_name
),

school_confirmed_children as (
  -- TODO: Integrate Platform Commons (PC) data for confirmed child counts
  select s.school_id, s.school_name, m.confirmed_child_count as confirmed_children
  from schools_with_mou s
  inner join {{ ref('dim_mou') }} m on s.school_id = m.partner_id and m.mou_status = 'active'
),

school_actual_children as (
  -- TODO: Integrate Platform Commons (PC) data for active child tracking
  select s.school_id, s.school_name, count(distinct ccs.child_id) as children_in_system
  from schools_with_mou s
  left join {{ ref('int_bubble__school_class') }} sc on s.school_id = sc.school_id and sc.is_removed = false
  left join {{ ref('bridge_child_class_section') }} ccs on sc.school_class_id = ccs.class_section_id and ccs.is_removed = false
  group by s.school_id, s.school_name
)

select 
  s.school_id, s.school_name,
  coalesce(svc.classes_with_volunteers, 0) as classes_with_volunteers,
  coalesce(svc.classes_without_volunteers, 0) as classes_without_volunteers,
  scc.confirmed_children,
  coalesce(sac.children_in_system, 0) as children_in_system
from schools_with_mou s
left join school_volunteer_counts svc on s.school_id = svc.school_id
left join school_confirmed_children scc on s.school_id = scc.school_id
left join school_actual_children sac on s.school_id = sac.school_id
order by s.school_name
