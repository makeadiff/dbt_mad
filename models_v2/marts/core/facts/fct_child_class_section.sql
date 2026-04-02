-- fct_child_class_section: Assignment of children to class sections
-- Grain: One record per child assigned to one class section in one academic year

select
    child_class_section_sk,
    child_sk,
    class_section_sk,
    child_class_section_id,
    child_id,
    class_section_id,
    academic_year,
    case 
        when is_removed = false then (current_date - created_date::date)
        else (modified_date::date - created_date::date)
    end as days_in_class,
    not is_removed as is_active_assignment,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__child_class_section') }}
where is_removed = false
