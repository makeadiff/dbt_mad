-- The "All districts" row equals the sum of the district rows on every count, and covers all
-- chapters in prod_do_chapter_summary (spec §3.6b).
with districts as (
    select
        sum(chapters) as chapters,
        sum(active_children) as active_children,
        sum(children_covered) as children_covered,
        sum(chapters_no_slots) as chapters_no_slots,
        sum(chapters_setup_complete) as chapters_setup_complete,
        sum(volunteers_minimum_to_start) as volunteers_minimum_to_start,
        sum(volunteers_needed) as volunteers_needed,
        sum(leads_chapter_link) as leads_chapter_link,
        sum(volunteers_in_chapter) as volunteers_in_chapter,
        sum(volunteers_allocated) as volunteers_allocated,
        sum(chapters_met_1to5) as chapters_met_1to5,
        sum(mentoring_circles_started) as mentoring_circles_started,
        sum(mentoring_circles_due) as mentoring_circles_due
    from {{ ref('prod_do_district_summary') }}
    where district <> 'All districts'
),

all_row as (
    select * from {{ ref('prod_do_district_summary') }}
    where district = 'All districts'
)

select a.district
from all_row a
cross join districts d
where a.chapters <> d.chapters
   or a.chapters <> (select count(*) from {{ ref('prod_do_chapter_summary') }})
   or a.active_children <> d.active_children
   or a.children_covered <> d.children_covered
   or a.chapters_no_slots <> d.chapters_no_slots
   or a.chapters_setup_complete <> d.chapters_setup_complete
   or a.volunteers_minimum_to_start <> d.volunteers_minimum_to_start
   or a.volunteers_needed <> d.volunteers_needed
   or a.leads_chapter_link <> d.leads_chapter_link
   or a.chapters_setup_complete <> (select count(*) filter (where setup_complete) from {{ ref('prod_do_chapter_summary') }})
   or a.volunteers_minimum_to_start <> (select sum(volunteers_minimum_to_start) from {{ ref('prod_do_chapter_summary') }})
   or a.leads_chapter_link <> (select sum(leads_chapter_link) from {{ ref('prod_do_chapter_summary') }})
   or a.volunteers_in_chapter <> d.volunteers_in_chapter
   or a.volunteers_allocated <> d.volunteers_allocated
   or a.chapters_met_1to5 <> d.chapters_met_1to5
   or a.mentoring_circles_started <> d.mentoring_circles_started
   or a.mentoring_circles_due <> d.mentoring_circles_due
union all
select 'missing All districts row' where not exists (select 1 from all_row)
