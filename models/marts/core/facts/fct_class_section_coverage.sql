{{ config(materialized='table') }}

-- fct_class_section_coverage: B3 (§4) -- how many volunteers are currently on each class section.
-- Grain: one row per class_section_id (dim_class_section). volunteers_on_class is currently
-- assigned, active volunteers only -- matches D1(a)'s "Allocated" filter (scsv.is_active = true,
-- not just is_removed = false). Buckets (0 / 1 / 2 / 3+) fall out downstream from
-- volunteers_on_class rather than being hard-coded here, per §9.

with volunteers_per_section as (
    select
        scs.class_section_id,
        count(distinct scsv.volunteer_id) as volunteers_on_class
    from {{ ref('int_bubble__slot_class_section') }} scs
    inner join {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
        on scsv.slot_class_section_id = scs.slot_class_section_id
    where scs.is_removed = false
      and scsv.is_removed = false
      and scsv.is_active = true
    group by scs.class_section_id
)

select
    cs.class_section_id,
    cs.school_id,
    cs.academic_year,
    coalesce(vps.volunteers_on_class, 0) as volunteers_on_class
from {{ ref('dim_class_section') }} cs
left join volunteers_per_section vps
    on cs.class_section_id = vps.class_section_id
