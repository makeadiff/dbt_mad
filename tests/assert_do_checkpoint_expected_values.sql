{{ config(severity='warn') }}

-- Expected values checked against the frontend (spec §4). Warn, never error: live data moves,
-- and a changed count must not fail a prod run. A warning means re-check the chapter on the
-- frontend, not that the model is wrong.
with expected (chapter_id, mentoring_circles_total, mentoring_circles_with_slot, active_children) as (
    values
        ('427', 21, 21, 93),
        ('604', 16, 12, 76),
        ('56', 14, 8, 60),
        ('554', 26, 26, 71),
        ('443', 14, 14, 50)
)
select e.chapter_id, e.mentoring_circles_total as expected_total, s.mentoring_circles_total,
    e.mentoring_circles_with_slot as expected_circles, s.mentoring_circles_with_slot,
    e.active_children as expected_children, s.active_children
from expected e
left join {{ ref('prod_do_chapter_summary') }} s
    on e.chapter_id = s.chapter_id
where s.chapter_id is null
   or s.mentoring_circles_total <> e.mentoring_circles_total
   or s.mentoring_circles_with_slot <> e.mentoring_circles_with_slot
   or s.active_children <> e.active_children
