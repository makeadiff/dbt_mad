{{ config(materialized='table') }}

-- fct_volunteer_assignments: Assignment of volunteers to slots and class sections
-- Flow: int_bubble__slot_class_section_volunteer → fct_volunteer_assignments

select
    volunteer_assignment_sk,
    slot_class_section_sk,
    volunteer_sk,
    slot_class_section_volunteer_id,
    slot_class_section_id,
    volunteer_id,
    academic_year,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__slot_class_section_volunteer') }}
where is_removed = false
