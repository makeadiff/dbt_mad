-- Volunteers needed is 1 per mentoring circle, the 1:5 requirement (spec §3.5b).
select chapter_id, volunteers_needed, mentoring_circles_total
from {{ ref('prod_do_chapter_summary') }}
where volunteers_needed <> mentoring_circles_total
