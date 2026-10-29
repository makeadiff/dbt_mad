-- Chapter planned / held on the chapter display = weekly rows summed by chapter from 1 Oct
-- (spec §3.6d). Chapters are matched on chapter, city and district since the display has no id.
with weekly as (
    select chapter_name, city, district, sum(sessions_planned) as planned, sum(sessions_held) as held
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1, 2, 3
)

select c."Chapter"
from {{ ref('prod_do_chapters_display') }} c
left join weekly w
    on c."Chapter" = w.chapter_name
   and c."City" is not distinct from w.city
   and c."District" is not distinct from w.district
where c."Sessions planned since 1 October" <> coalesce(w.planned, 0)
   or c."Sessions held since 1 October" <> coalesce(w.held, 0)
