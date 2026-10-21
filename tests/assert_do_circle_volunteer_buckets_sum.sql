-- Circles with 0 + 1 + 2+ volunteers = circles with a slot, per chapter.
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where mentoring_circles_0_volunteers + mentoring_circles_1_volunteer + mentoring_circles_2plus_volunteers
      <> mentoring_circles_with_slot
