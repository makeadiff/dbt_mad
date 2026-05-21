{{ config(materialized='table') }}

-- bridge_child_class_section: Many-to-many relationship between children and class sections
-- Source: int_bubble__child_class_section

select
    child_class_section_id,
    child_id,
    class_section_id,
    academic_year,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__child_class_section') }}
