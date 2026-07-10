{{ config(
  materialized='table'
) }}

-- CTE1: Get all schools with active MOU agreements
with schools_with_mou as (
  select distinct
    m.partner_id as school_id,
    p.partner_name as school_name
  from {{ ref('mous_int') }} m
  inner join {{ ref('partners_int') }} p
    on m.partner_id::int = p.id::int
  where m.mou_status = 'active'
    and p.removed = 'FALSE'
),

-- CTE2: Get slot information mapped to schools with MOU
school_slots as (
  select distinct
    s.school_id,
    s.school_name,
    sl.slot_id
  from schools_with_mou s
  left join {{ ref('slot_int') }} sl
    on s.school_id::int = sl.school_id::int
    and sl.removed = 'FALSE'
),

-- CTE3: Get class sections for each slot
slot_class_sections as (
  select 
    ss.school_id,
    ss.school_name,
    ss.slot_id,
    scs.slot_class_section_id as class_section_id
  from school_slots ss
  left join {{ ref('slot_class_section_int') }} scs
    on ss.slot_id = scs.slot_id
    and scs.removed = 'FALSE'
),

-- CTE4: Count volunteers per class section
class_section_volunteers as (
  select 
    scs.school_id,
    scs.school_name,
    scs.class_section_id,
    case 
      when scsv.volunteer_id is not null then 1 
      else 0 
    end as has_volunteer
  from slot_class_sections scs
  left join {{ ref('slot_class_section_volunteer_int') }} scsv
    on scs.class_section_id = scsv.slot_class_section_id
    and scsv.removed = 'FALSE'
),

-- CTE5: Get volunteer counts per school
school_volunteer_counts as (
  select 
    school_id,
    school_name,
    count(case when has_volunteer = 1 then 1 end) as classes_with_volunteers,
    count(case when has_volunteer = 0 then 1 end) as classes_without_volunteers
  from class_section_volunteers
  group by school_id, school_name
),

-- CTE6: Get confirmed children count from MOU
school_confirmed_children as (
  select 
    s.school_id,
    s.school_name,
    m.confirmed_child_count as confirmed_children
  from schools_with_mou s
  inner join {{ ref('mous_int') }} m
    on s.school_id::int = m.partner_id::int
    and m.mou_status = 'active'
),

-- CTE7: Get actual children count from child_class
school_actual_children as (
  select 
    s.school_id,
    s.school_name,
    count(distinct cc.child_id) as children_in_system
  from schools_with_mou s
  left join {{ ref('school_class_int') }} sc
    on s.school_id::int = sc.school_id::int
    and sc.removed = 'FALSE'
  left join {{ ref('child_class_int') }} cc
    on sc.school_class_id = cc.school_class_id
    and cc.removed_boolean = 'FALSE'
  group by s.school_id, s.school_name
)

-- Final output: Combine all metrics per school (only schools with active MOU)
select 
  s.school_id,
  s.school_name,
  coalesce(svc.classes_with_volunteers, 0) as classes_with_volunteers,
  coalesce(svc.classes_without_volunteers, 0) as classes_without_volunteers,
  scc.confirmed_children,
  coalesce(sac.children_in_system, 0) as children_in_system
from schools_with_mou s
left join school_volunteer_counts svc
  on s.school_id = svc.school_id
left join school_confirmed_children scc
  on s.school_id = scc.school_id
left join school_actual_children sac
  on s.school_id = sac.school_id
order by s.school_name
