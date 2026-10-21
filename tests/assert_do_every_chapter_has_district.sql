-- Every active E2 chapter has a district and DO from seed_do_chapter_district. A miss means a
-- chapter joined the 62 without a seed row: add it to the seed.
select chapter_id, chapter_name
from {{ ref('prod_do_chapter_summary') }}
where district is null or do_name is null
