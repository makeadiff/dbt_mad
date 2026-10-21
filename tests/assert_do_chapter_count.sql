{{ config(severity='warn') }}

-- 62 active E2 chapters at the 2026-10-22 checkpoint. Warn only: the ops sheet can add or drop
-- a chapter, which is a check for Akshay, not a broken model.
select count(*) as n
from {{ ref('prod_do_chapter_summary') }}
having count(*) <> 62
