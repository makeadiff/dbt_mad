-- Every CCI has a district (8-11) and a DO, in the seed and in the chapter summary (spec E1 §7 D1).
-- Returns the CCIs that don't.
select school_id::text as chapter_id
from {{ ref('seed_cci_chapter_group') }}
where district not in ('District 8', 'District 9', 'District 10', 'District 11')
   or coalesce(trim(do_name), '') = ''
union all
select chapter_id
from {{ ref('prod_cci_chapter_summary') }}
where district is null or coalesce(trim(do_name), '') = ''
