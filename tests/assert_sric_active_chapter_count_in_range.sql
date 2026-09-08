-- 2026-09-08: backstop against a silent collapse to zero. chapter_status ultimately depends on
-- int_google_sheet__chapter_mapping, which is a straight table materialization over a raw Airbyte
-- destination that gets wholesale-overwritten on each sync -- a failed or empty sync leaves no
-- error, no failing test, just an empty table, and every chapter's chapter_status (and previously
-- the whole e2_chapters grain) reads false/zero with the dashboard otherwise rendering fine.
-- Expected active count is 63 (see prod_sric_dashboard_data's e2_chapter_status header); this
-- fails the build if the count falls outside a sane band, so a collapse breaks loudly instead of
-- shipping a dashboard full of zeros.
select count(*) as active_chapter_count
from {{ ref('prod_sric_dashboard_data') }}
where chapter_status = true
having count(*) < 40 or count(*) > 100
