-- fct_child_class_section: Assignment of children to class sections
-- Grain: One record per child assigned to one class section in one academic year

select
    ccs.child_class_section_sk,
    ccs.child_sk,
    ccs.class_section_sk,
    ccs.child_class_section_id,
    ccs.child_id,
    ccs.class_section_id,
    cs.school_academic_year_id,
    case
        when ccs.is_removed = false then (current_date - ccs.created_date::date)
        else (ccs.modified_date::date - ccs.created_date::date)
    end as days_in_class,
    not ccs.is_removed as is_active_assignment,
    ccs.is_removed,
    ccs.created_date,
    ccs.modified_date
from {{ ref('int_bubble__child_class_section') }} ccs
left join {{ ref('int_bubble__class_section') }} cs
    on ccs.class_section_id = cs.class_section_id
where ccs.is_removed = false
