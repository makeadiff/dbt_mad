{{
  config(
    materialized='table',
    description='SRIC funnel data unpivoted to stage-grained rows for Dalgo bar charts (X-axis needs a dimension, not one bar per metric)'
  )
}}

-- prod_sric_funnel: unpivots fct_volunteer_pipeline into stage-grained rows so Dalgo's bar chart
-- -- which requires an X-axis dimension plus metrics and cannot render one bar per metric -- can
-- plot the funnel as rows instead of columns. This model reshapes existing, already-validated
-- counts for charting; it adds no new business logic.
--
-- Grain: one row per (funnel_block, stage, chapter_id, volunteer_source) -- dense, not sparse.
-- Every chapter that has any volunteer in a block gets a row for every stage of that block (and,
-- in the coverage block, every volunteer_source), volunteers = 0 where none qualify (e.g.
-- Onboarded/Active in class are 0 nearly everywhere today). A naive GROUP BY + WHERE would drop
-- those stages as missing rows entirely, which is the same zero-vs-missing confusion D11 exists
-- to prevent (§3.5) -- a bar that silently isn't there reads as "no data queried", not
-- "genuinely zero".
--
-- funnel_block = 'intake' | 'coverage' are DIFFERENT POPULATIONS (§12c) and must never be
-- compared or converted across each other in a chart or a percentage: intake is a cohort flow
-- (this year's applicant cohort, opportunity-filtered), coverage is a current-state census
-- (everyone currently active, any year, no opportunity filter). A conversion between them
-- produces figures above 100% -- the same error class as the original D1 405 > 184. Filter or
-- facet by funnel_block; do not sum or divide across it.
--
-- volunteer_source (coverage block ONLY; NULL on intake rows -- intake is all new by definition,
-- so a split there would be fake, not informative):
--   'new_this_year' -- is_recruited_new = true (this year's 26-27 hires)
--   'continuing'    -- everyone else in coverage. Deliberately not "retained": it also covers
--                       volunteers with no clean PC hire record on file, so the label stays
--                       honest about what it actually knows.
-- This exists because retained volunteers correctly appear in Coverage but never in Intake,
-- leaving an unexplained gap between Intake's Recruited and Coverage's Allocated to School.
-- Splitting Coverage by source explains that gap and shows where continuing volunteers drop off
-- stage by stage -- e.g. continuing volunteers falling away at Compliant would surface unchased
-- annual CPP re-signatures, invisible in the combined count.
--
-- Stage names follow MAD vocabulary (renamed 2026-08-27): "school" vs "class" is the documented
-- cause of last year's metric discrepancy (§6.11/§6.8 -- volunteers allocated to a school are not
-- the same population as volunteers allocated to a class), so both stage names and the underlying
-- fct_volunteer_pipeline booleans now say which one explicitly rather than leaving it implicit.
--
-- chapter/city come from prod_sric_dashboard_data (one authoritative name per chapter_id, same
-- as every other panel). Rows with chapter_id = NULL (unattributed intake leads -- open_pool,
-- city, chapter_unmatched -- see fct_volunteer_pipeline's KNOWN GAP) collapse into one row per
-- stage spanning many cities, so chapter/city are correctly NULL there rather than guessed at;
-- the city-level breakdown for those leads lives in Panel 1's supply strip instead.
--
-- conversion_pct (§14 Panel 3/4, §6.10 display consequence): stage-over-stage conversion,
-- computed per chapter_id, INTAKE ONLY. Intake's four stages are a genuine cohort sequence, so a
-- conversion is a real number. Coverage's five stages are independent states, not a sequence
-- (§6.10) -- a conversion between them would be fabricated, so this is NULL for every coverage
-- row rather than left for Dalgo to compute by accident. No metric logic in the BI layer (§14).

with base as (
    select
        chapter_id,
        volunteer_id,
        is_draft,
        is_applied,
        is_completed,
        is_recruited_new,
        is_allocated_to_school,
        is_allocated_to_class,
        is_compliant,
        is_onboarded,
        is_active_in_class
    from {{ ref('fct_volunteer_pipeline') }}
),

chapter_names as (
    select distinct
        chapter_id,
        chapter,
        city
    from {{ ref('prod_sric_dashboard_data') }}
),

intake_stage_dim as (
    select 1 as stage_order, 'Draft' as stage_name
    union all select 2, 'Applied'
    union all select 3, 'Application Complete'
    union all select 4, 'Recruited'
),

coverage_stage_dim as (
    select 1 as stage_order, 'Allocated to School' as stage_name
    union all select 2, 'Allocated to Class'
    union all select 3, 'Compliant'
    union all select 4, 'Onboarded'
    union all select 5, 'Active in Class'
),

coverage_source_dim as (
    select 'new_this_year' as volunteer_source
    union all select 'continuing'
),

-- Chapters relevant to each block: any chapter with at least one volunteer somewhere in that
-- block's stages -- the dense chapter x stage grid is built from this, not from every chapter
-- that exists (a chapter with zero intake or coverage presence isn't part of this funnel).
intake_chapters as (
    select distinct chapter_id
    from base
    where is_draft or is_applied or is_completed or is_recruited_new
),

coverage_chapters as (
    select distinct chapter_id
    from base
    where is_allocated_to_school or is_allocated_to_class or is_compliant or is_onboarded or is_active_in_class
),

intake_counts as (
    select 'intake' as funnel_block, d.stage_order, d.stage_name, ic.chapter_id,
        cast(null as text) as volunteer_source,
        count(distinct b.volunteer_id) as volunteers
    from intake_chapters ic
    cross join intake_stage_dim d
    left join base b
        on b.chapter_id is not distinct from ic.chapter_id
        and (
            (d.stage_order = 1 and b.is_draft)
            or (d.stage_order = 2 and b.is_applied)
            or (d.stage_order = 3 and b.is_completed)
            or (d.stage_order = 4 and b.is_recruited_new)
        )
    group by d.stage_order, d.stage_name, ic.chapter_id
),

coverage_counts as (
    select 'coverage' as funnel_block, d.stage_order, d.stage_name, cc.chapter_id,
        sd.volunteer_source,
        count(distinct b.volunteer_id) as volunteers
    from coverage_chapters cc
    cross join coverage_stage_dim d
    cross join coverage_source_dim sd
    left join base b
        on b.chapter_id is not distinct from cc.chapter_id
        and (b.is_recruited_new = (sd.volunteer_source = 'new_this_year'))
        and (
            (d.stage_order = 1 and b.is_allocated_to_school)
            or (d.stage_order = 2 and b.is_allocated_to_class)
            or (d.stage_order = 3 and b.is_compliant)
            or (d.stage_order = 4 and b.is_onboarded)
            or (d.stage_order = 5 and b.is_active_in_class)
        )
    group by d.stage_order, d.stage_name, cc.chapter_id, sd.volunteer_source
),

all_stages as (
    select * from intake_counts
    union all
    select * from coverage_counts
),

with_conversion as (
    select
        a.*,
        lag(a.volunteers) over (
            partition by a.chapter_id order by a.stage_order
        ) as prev_stage_volunteers
    from all_stages a
)

select
    a.funnel_block,
    a.stage_order,
    a.stage_name,
    a.chapter_id,
    cn.chapter,
    cn.city,
    a.volunteer_source,
    a.volunteers,
    case
        when a.funnel_block = 'intake'
             and a.prev_stage_volunteers is not null
             and a.prev_stage_volunteers > 0
        then round(100.0 * a.volunteers / a.prev_stage_volunteers, 1)
    end as conversion_pct
from with_conversion a
left join chapter_names cn
    on a.chapter_id = cn.chapter_id
