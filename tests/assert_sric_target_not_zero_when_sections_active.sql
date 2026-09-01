-- D11 / §3.5: a target of 0 should mean "no active class sections", never a broken join. Fails
-- (returns rows) if a chapter has active class sections but a zero recruitment target -- the
-- exact symptom that made D5/D6/D7 indistinguishable from a genuine zero.
select chapter_id, chapter, active_class_sections, volunteers_required
from {{ ref('prod_sric_dashboard_data') }}
where classes_set_up = true
  and volunteers_required = 0
