-- Every active E2 chapter in dim_chapter_mapping appears in prod_do_chapter_summary, and nothing
-- else does. Chapters not met stay in the denominator, so a dropped chapter would inflate the %.
select coalesce(m.chapter_id, s.chapter_id) as chapter_id, m.chapter_id is null as extra_row
from (
    select chapter_id from {{ ref('dim_chapter_mapping') }}
    where engine = 'E2' and chapter_status = 'Active'
) m
full outer join {{ ref('prod_do_chapter_summary') }} s
    on m.chapter_id = s.chapter_id
where m.chapter_id is null or s.chapter_id is null
