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
-- Onboarded/Ready to Mentor Children are 0 nearly everywhere today). A naive GROUP BY + WHERE
-- would drop those stages as missing rows entirely, which is the same zero-vs-missing confusion
-- D11 exists to prevent (§3.5) -- a bar that silently isn't there reads as "no data queried", not
-- "genuinely zero".
--
-- COVERAGE STAGE ORDER, REVISED 2026-08-27, follows the real process rather than the model build
-- order: compliance and induction gate class allocation, so Compliant and Onboarded now sit
-- BEFORE Allocated to Class (1 Allocated to School, 2 Compliant, 3 Onboarded, 4 Allocated to
-- Class, 5 Ready to Mentor Children). This is a display/diagnostic reordering only -- the stages
-- are still independent, overlapping states (§6.10), not a sequence.
--
-- DIAGNOSTIC CONSEQUENCE, INTENDED: with this order, Allocated to Class (48) sitting ABOVE
-- Compliant (6) is now visible as a process violation -- volunteers placed with children before
-- clearing CPP/COC. Do NOT "fix" this by reordering the stages back to build order; that would
-- hide the exact thing this reorder exists to surface.
--
-- funnel_block = 'intake' | 'coverage' are DIFFERENT POPULATIONS (§12c) and must never be
-- compared or converted across each other in a chart or a percentage: intake is this year's
-- applicant cohort (opportunity-filtered), coverage is a current-state census (everyone currently
-- active, any year, no opportunity filter). A conversion between them produces figures above
-- 100% -- the same error class as the original D1 405 > 184. Filter or facet by funnel_block; do
-- not sum or divide across it.
--
-- NEITHER BLOCK IS A FUNNEL (§6.10, revised 2026-08-27 after verifying is_draft/is_applied/
-- is_completed are mutually exclusive -- 0 volunteers have more than one true, confirmed against
-- fct_volunteer_pipeline). stage_relationship marks what each block actually is:
--   'exclusive'   (intake)   -- Draft/Applied/Completed are the three non-exit values of the
--                                single ApplicationStatus field. A volunteer is in exactly one.
--                                This is a STATUS DISTRIBUTION, not a sequence -- "progressed
--                                from Draft to Applied" is not a claim the data supports.
--                                Recruited (CurrentStepStatus = HIRE) is a fourth, independent
--                                flag layered on top and CAN co-occur with Completed -- that pair
--                                is the one genuine progression in this block (see conversion_pct
--                                below).
--   'overlapping' (coverage) -- five independent states from different systems that do not nest
--                                (§6.10): Compliant > Allocated to Class is real and correct.
-- Do not chart either block as a left-to-right funnel implying progression through all stages.
--
-- volunteer_source (coverage block ONLY; NULL on intake rows -- intake is all new by definition,
-- so a split there would be fake, not informative). Display-ready values (2026-08-27) -- Dalgo
-- renders raw column values as legend labels, so these are the actual legend text, not codes:
--   'New this year'              -- is_recruited_new = true (this year's 26-27 hires)
--   'Continuing from last year'  -- everyone else in coverage. Deliberately not "Retained": it
--                                    also covers volunteers with no clean PC hire record on file,
--                                    so the label stays honest about what it actually knows.
-- Joined via the stable is_new_this_year boolean in coverage_source_dim, not by matching this
-- display string (§6.1b) -- a future copy change to the legend text must not silently break the
-- join the way the stage_name order-prefix broke a test that matched on it.
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
-- conversion_pct (§14 Panel 3/4, §6.10 display consequence, revised 2026-08-27): non-NULL for
-- exactly one transition -- Completed -> Recruited, the only genuine progression in either block
-- (see stage_relationship above). NULL everywhere else: Draft/Applied/Completed are exclusive
-- states of one distribution, not sequential stages, so a "Draft -> Applied conversion" would be
-- fabricated the same way a coverage conversion would be. No metric logic in the BI layer (§14).
--
-- stage_name is prefixed with its order ("1 · Draft") because Dalgo's bar chart sorts its X axis
-- alphabetically, not by a hidden order column (confirmed 2026-08-27) -- stage_order is kept as
-- its own column for models/tests, but the chart needs the order encoded in the label itself.

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
        is_ready_to_mentor
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
    select 1 as stage_order, '1 · Draft' as stage_name
    union all select 2, '2 · Applied'
    union all select 3, '3 · Application Complete'
    union all select 4, '4 · Recruited'
),

coverage_stage_dim as (
    select 1 as stage_order, '1 · Allocated to School' as stage_name
    union all select 2, '2 · Compliant'
    union all select 3, '3 · Onboarded'
    union all select 4, '4 · Allocated to Class'
    union all select 5, '5 · Ready to Mentor Children'
),

-- is_new_this_year is the stable join key; volunteer_source is the display label only (Dalgo
-- renders raw column values as legend labels -- §6.1b: never join on a display string, it will
-- go stale invisibly the next time the label copy changes).
coverage_source_dim as (
    select true as is_new_this_year, 'New this year' as volunteer_source
    union all select false, 'Continuing from last year'
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
    where is_allocated_to_school or is_allocated_to_class or is_compliant or is_onboarded or is_ready_to_mentor
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
        and (b.is_recruited_new = sd.is_new_this_year)
        and (
            (d.stage_order = 1 and b.is_allocated_to_school)
            or (d.stage_order = 2 and b.is_compliant)
            or (d.stage_order = 3 and b.is_onboarded)
            or (d.stage_order = 4 and b.is_allocated_to_class)
            or (d.stage_order = 5 and b.is_ready_to_mentor)
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
    case when a.funnel_block = 'intake' then 'exclusive' else 'overlapping' end as stage_relationship,
    a.stage_order,
    a.stage_name,
    a.chapter_id,
    cn.chapter,
    cn.city,
    a.volunteer_source,
    a.volunteers,
    -- Only Completed -> Recruited (intake stage_order 4) is a genuine progression -- see header.
    case
        when a.funnel_block = 'intake'
             and a.stage_order = 4
             and a.prev_stage_volunteers is not null
             and a.prev_stage_volunteers > 0
        then round(100.0 * a.volunteers / a.prev_stage_volunteers, 1)
    end as conversion_pct
from with_conversion a
left join chapter_names cn
    on a.chapter_id = cn.chapter_id
