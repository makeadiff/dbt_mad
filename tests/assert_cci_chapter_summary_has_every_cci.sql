-- Every CCI in the seed (28 schools with a 2026-27 section, spec E1 §7 ruling 2) appears once in
-- prod_cci_chapter_summary, and nothing else does. Returns the mismatches.
select coalesce(c.school_id::text, s.chapter_id) as chapter_id
from {{ ref('seed_cci_chapter_group') }} c
full join {{ ref('prod_cci_chapter_summary') }} s
    on c.school_id::text = s.chapter_id
where c.school_id is null
   or s.chapter_id is null
