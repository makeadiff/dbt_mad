-- A chapter with all mentoring circles started has sessions started (spec §3.5b).
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where sessions_commenced_met and not has_sessions_started
