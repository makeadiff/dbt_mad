{{ config(materialized='table') }}

-- bridge_child_class_section: Many-to-many relationship between children and class sections
-- Source: int_bubble__child_class_section

select
    ccs.child_class_section_id,
    ccs.child_id,
    ccs.class_section_id,
    cs.school_academic_year_id,
    ccs.is_removed,
    ccs.created_date,
    ccs.modified_date
from {{ ref('int_bubble__child_class_section') }} ccs
left join {{ ref('int_bubble__class_section') }} cs
    on ccs.class_section_id = cs.class_section_id
