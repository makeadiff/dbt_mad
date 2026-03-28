{{ config(materialized='table') }}

-- dim_class_section: One row per class section
-- Flow: int_bubble__class_section + int_bubble__school_class + stg_bubble__class → dim_class_section

select
    cs.class_section_id,
    cs.section_name,
    c.class_name,
    cs.school_class_id,
    cs.school_id,
    cs.academic_year,
    cs.is_active,
    cs.is_removed,
    cs.created_date,
    cs.modified_date
from {{ ref('int_bubble__class_section') }} cs
left join {{ ref('int_bubble__school_class') }} sc
    on cs.school_class_id = sc.school_class_id
left join {{ ref('stg_bubble__class') }} c
    on sc.class_id = c.class_id
where cs.is_removed = false
