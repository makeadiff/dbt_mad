{{ config(materialized='table') }}

-- bridge_child_subject: Child-to-subject enrollment relationship
-- Source: int_bubble__class_section_subject + int_bubble__child_class_section

select
    ccs.child_id,
    css.class_section_subject_id,
    css.subject_id,
    css.class_section_id,
    css.academic_year,
    css.is_removed,
    css.created_date,
    css.modified_date
from {{ ref('int_bubble__class_section_subject') }} css
inner join {{ ref('int_bubble__child_class_section') }} ccs
    on css.class_section_id = ccs.class_section_id
    and ccs.is_removed = false
where css.is_removed = false
