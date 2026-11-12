{{ config(severity='warn') }}
-- E1 spec §7 B2: every chapter spelling on the E1 baseline sheet is in seed_cci_baseline_chapter_name,
-- either as a CCI (school_id) or as a known School / Community chapter (blank school_id). A row here
-- is a new or unusable spelling whose forms are not counted: add it to the seed once its CCI is
-- confirmed, never by guessing. Warn so a new spelling doesn't block the build.
with names as (
    select
        case when trim(chapter_name_raw) = 'Chapter not listed above'
             then coalesce(chapter_name_other, '(blank)')
             else coalesce(nullif(trim(chapter_name_raw), ''), '(blank)')
        end as sheet_chapter_name
    from {{ ref('stg_google_sheets__baseline_responses') }}
)

select n.sheet_chapter_name, count(*) as forms
from names n
left join {{ ref('seed_cci_baseline_chapter_name') }} s
    on n.sheet_chapter_name = trim(s.sheet_chapter_name)
where s.sheet_chapter_name is null
group by 1
