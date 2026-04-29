-- fct_volunteer_assignments: Assignment of volunteers to slots and class sections
-- Grain: One record per volunteer assigned to one teaching slot in one academic year

select
    volunteer_assignment_sk,
    slot_class_section_sk,
    volunteer_sk,
    slot_class_section_volunteer_id,
    slot_class_section_id,
    volunteer_id,
    academic_year,
    case 
        when is_removed = false then (current_date - created_date::date)
        else (modified_date::date - created_date::date)
    end as tenure_in_slot,
    not is_removed as is_active_assignment,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__slot_class_section_volunteer') }}
where is_removed = false
