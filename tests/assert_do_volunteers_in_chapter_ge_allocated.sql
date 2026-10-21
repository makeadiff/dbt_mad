-- Every allocated volunteer is in the chapter (spec §3.5b), so in the chapter >= allocated.
select chapter_id, volunteers_in_chapter, volunteers_allocated
from {{ ref('prod_do_chapter_summary') }}
where volunteers_in_chapter < volunteers_allocated
