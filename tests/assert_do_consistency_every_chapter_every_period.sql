-- prod_do_volunteer_consistency has every chapter for every period (62 x periods), so a period
-- filter in Dalgo never silently drops a chapter.
select period_type, period_start, count(*) as chapters
from {{ ref('prod_do_volunteer_consistency') }}
group by 1, 2
having count(*) <> (select count(*) from {{ ref('prod_do_chapter_summary') }})
