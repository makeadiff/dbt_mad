-- Grain enforcement (2026-09-12): prod_sric_dashboard_data must be exactly one row per chapter_id
-- -- every consumer already assumes this (prod_sric_funnel defends with SELECT DISTINCT;
-- prod_chapter_campaign_daily does not, and would silently double-render a duplicated active
-- chapter's whole campaign window). The model now dedupes (chapter 529's two identical sheet rows)
-- and excludes chapter_id IS NULL rows (five orphaned sheet rows) -- this closes the SUM()
-- fragility those left behind rather than relying on it staying incidental.
select chapter_id, count(*) as row_count
from {{ ref('prod_sric_dashboard_data') }}
group by chapter_id
having count(*) > 1
