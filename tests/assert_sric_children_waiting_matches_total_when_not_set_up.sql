-- children_waiting sanity check (2026-09-12): a chapter with no live-slot class sections
-- (classes_set_up = false) has nowhere for any enrolled child to be placed, so every one of its
-- active children must be waiting -- children_waiting should exactly equal total_children.
-- A mismatch here means the live-slot section population children_waiting subtracts against has
-- drifted from the one classes_set_up uses, and the two numbers would stop agreeing on what
-- counts as a functioning class.
select chapter_id, chapter, total_children, children_waiting
from {{ ref('prod_sric_dashboard_data') }}
where classes_set_up = false
  and children_waiting <> total_children
