-- A chapter flagged setup complete has 0 children outside a mentoring circle and 0 in a circle
-- with no weekly slot; a chapter with data this year and both at 0 is flagged (spec §3.8).
-- Minimum volunteers = ceil(active children / 5).
select chapter_id, setup_complete, children_in_no_class_section, children_in_circle_no_slot
from {{ ref('prod_do_chapter_summary') }}
where (setup_complete and (children_in_no_class_section > 0 or children_in_circle_no_slot > 0))
   or (not setup_complete
       and volunteer_requirement_not_met_reason is distinct from 'no_data'
       and children_in_no_class_section = 0 and children_in_circle_no_slot = 0)
   or volunteers_minimum_to_start <> ceil(active_children / 5.0)
