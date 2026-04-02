{{ config(materialized='table') }}

-- fct_child_class_section: Assignment of children to class sections
-- Flow: int_bubble__child_class_section → fct_child_class_section

select
    child_class_section_sk,
    child_sk,
    class_section_sk,
    child_class_section_id,
    child_id,
    class_section_id,
    academic_year,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__child_class_section') }}
where is_removed = false
