-- Planned >= held everywhere, and "no attendance logged" = planned - held (spec §3.6d), on the
-- district display, chapter display and weekly display.
select 'district' as model, "District" as name
from {{ ref('prod_do_district_display') }}
where "Sessions planned since 1 October" < "Sessions held since 1 October"
   or "Sessions with no attendance logged since 1 October"
      <> "Sessions planned since 1 October" - "Sessions held since 1 October"
union all
select 'chapter', "Chapter"
from {{ ref('prod_do_chapters_display') }}
where "Sessions planned since 1 October" < "Sessions held since 1 October"
   or "Sessions with no attendance logged since 1 October"
      <> "Sessions planned since 1 October" - "Sessions held since 1 October"
union all
select 'weekly', "Chapter" || ' ' || "Week starting"
from {{ ref('prod_do_weekly_sessions_display') }}
where "Sessions planned" < "Sessions held"
   or "Sessions with no attendance logged" <> "Sessions planned" - "Sessions held"
