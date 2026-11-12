{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The Baseline tab's chapter table (spec §3.4 B5): one row per chapter,
-- sourced from prod_do_baseline_chapters, with the same filter columns as prod_do_chapters_display
-- (Chapter, City, Chapter organiser, District, DO). Dalgo sorts on "Children with no form done",
-- most first. Counts children, not forms (one-line note on the tab), except the three
-- "forms submitted" columns, which count forms as the Dots report does (cards on the tab).
-- The 1 / 2 / 3 columns match the dashboard cards ("Children with 1 form done" etc., DO spec §8).

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    active_children                 as "Active children",
    children_numeracy_done          as "Children with numeracy done",
    children_literacy_done          as "Children with literacy done",
    children_life_skills_done       as "Children with life skills done",
    children_0_forms                as "Children with no form done",
    children_1_form                 as "Children with 1 form done",
    children_2_forms                as "Children with 2 forms done",
    children_3_forms                as "Children with all 3 forms done",
    pct_baseline_complete           as "% of children with baseline complete",
    forms_numeracy_submitted        as "Numeracy forms submitted",
    forms_literacy_submitted        as "Literacy forms submitted",
    forms_life_skills_submitted     as "Life skills forms submitted"
from {{ ref('prod_do_baseline_chapters') }}
