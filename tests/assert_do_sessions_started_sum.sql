-- Circles not yet due + started + not started = circles with a slot, per chapter (circles with no
-- slot are left out of Metric 2).
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where mentoring_circles_not_yet_due + mentoring_circles_started + mentoring_circles_not_started
      <> mentoring_circles_with_slot
