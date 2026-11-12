{{ config(materialized='table') }}

-- Why: the E1 baseline sheet is one row per form, keyed by free-text chapter and child names, with
-- no usable child ID (E1 spec §3.7). The CCI Baseline tab counts children per CCI, so this model
-- resolves each form to a CCI and collapses repeat forms for the same child, and drops the names.
-- Flow: stg_google_sheets__baseline_responses + seed_cci_baseline_chapter_name
--       -> int_google_sheet__cci_baseline_children
-- Downstream gets: one row per child per CCI with how many forms they have and whether they cleared
-- ASER, with no names (B1: only counts leave the raw layer).
--
-- Chapter (B2): the form's Chapter Name, trimmed; when it is "Chapter not listed above", the
-- free-text "enter the chapter name" answer instead (on 12 Nov, 3 rows, all Netaji Dehradun). The
-- free-text answer is ignored on rows with a listed chapter (it holds notes such as "DBU", "NA").
-- Mapped through the seed by exact trimmed spelling; seed rows with a blank school_id are School or
-- Community chapters (out of scope). Spellings in neither group are dropped here and reported by
-- the test assert_cci_baseline_chapter_names_mapped.
--
-- Child (B3): distinct (CCI, lower-trimmed student name, grade). The key is an md5 of name and
-- grade so the name itself never leaves staging. Rows with no student name are dropped (none on 12 Nov).
-- Cleared ASER (B4): "Yes" to "Did the child clear all the levels of the ASER Baseline?" on any of
-- the child's forms (the sheet's Dashboard tab counts "Yes" rows: 105 on 12 Nov).
--
-- Grain: one row per (school_id, child_key).

with forms as (
    select
        case when trim(chapter_name_raw) = 'Chapter not listed above'
             then chapter_name_other
             else nullif(trim(chapter_name_raw), '')
        end                                                         as sheet_chapter_name,
        md5(lower(trim(student_name)) || '|' || coalesce(student_grade, '')) as child_key,
        student_grade,
        aser_cleared_answer
    from {{ ref('stg_google_sheets__baseline_responses') }}
    where student_name is not null
),

mapped as (
    select
        s.school_id::text as school_id,
        f.child_key,
        f.student_grade,
        f.aser_cleared_answer
    from forms f
    join {{ ref('seed_cci_baseline_chapter_name') }} s
        on f.sheet_chapter_name = trim(s.sheet_chapter_name)
    where s.school_id is not null
)

select
    school_id,
    child_key,
    max(student_grade)                                   as student_grade,
    count(*)                                             as forms_submitted,
    bool_or(lower(aser_cleared_answer) = 'yes')          as cleared_aser
from mapped
group by 1, 2
