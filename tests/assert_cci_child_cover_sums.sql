-- Children with a volunteer + not yet in a circle + in a circle with no volunteer equal the active
-- children, per CCI (spec E1 §3.2).
select chapter_id
from {{ ref('prod_cci_chapter_summary') }}
where children_with_volunteer + children_not_in_circle + children_in_circle_no_volunteer <> active_children
