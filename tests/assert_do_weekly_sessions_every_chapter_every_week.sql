-- prod_do_weekly_sessions has every chapter for every week.
select week_start, count(*) as chapters
from {{ ref('prod_do_weekly_sessions') }}
group by 1
having count(*) <> (select count(*) from {{ ref('prod_do_chapter_summary') }})
