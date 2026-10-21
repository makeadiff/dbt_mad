-- Covered + in a circle with no volunteer + in a circle with no slot + in no class section
-- = active children, per chapter. Each child sits in exactly one (worst) group.
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where children_covered + children_in_circle_no_volunteer + children_in_circle_no_slot
      + children_in_no_class_section <> active_children
