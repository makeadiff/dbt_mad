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
--
-- COVERAGE STAGE_NAME VOCABULARY, FINALIZED 2026-09-12 (prod_sric_funnel_display): stage_name
-- values for the coverage block are now the final Dalgo-facing text -- "1 · Placed at a school",
-- "2 · Signed child safety policies", "3 · Attended induction", "4 · Placed in a class",
-- "5 · Ready to mentor" -- replacing the earlier build-order names (Allocated to School/Compliant/
-- Onboarded/Allocated to Class/Ready to Mentor Children) used in comments elsewhere in this file.
-- Those comments describe the underlying fct_volunteer_pipeline booleans (is_compliant,
-- is_allocated_to_class, etc.) and are left as-is; stage_order -- not stage_name text -- is what
-- every model/test here keys off, per §6.1b (see assert_sric_funnel_coverage_monotonic).
--
-- NEEDED REFERENCE ROW (2026-09-11, coverage block only): stage_order 0, stage_name "0 · Needed",
-- volunteers = that chapter's volunteers_required from prod_sric_dashboard_data, volunteer_source
-- NULL (a requirement isn't split new-vs-continuing -- there's one target, not two populations).
-- This is the coverage chart's title change to "Coverage vs requirement": every other stage now
-- reads against this leftmost bar instead of the chart just listing state counts. Needed is a
-- TARGET, not a state a volunteer occupies, so it deliberately sits outside
-- assert_sric_funnel_coverage_monotonic's state-nesting invariants -- do not add stage_order = 0
-- to that test.
--
-- THIRD BLOCK, LEAD FUNNEL (2026-09-12): funnel_block = 'intake_to_class'. Unlike intake
-- (exclusive states of one field) and coverage (independent overlapping states, §6.10), this block
-- genuinely IS a nested funnel -- each stage's condition is a strict AND-superset of the one
-- before it (Applied ⊇ Recruited ⊇ Placed at a School ⊇ Placed in a Class), so
-- stage_relationship = 'nested' and conversion_pct is meaningful (and computed) at every stage,
-- not just one pair. National only (chapter_id NULL on every row -- this is a single funnel across
-- the whole 26-27 cohort, not chapter-faceted) -- do not add a chapter dimension to this block
-- without first deciding whether "Applied" should be chapter-attributed (most applicants aren't,
-- per fct_volunteer_pipeline's lead_attribution gap). Scoped to the 26-27 cohort by construction:
-- every boolean it reads (is_applied/is_completed/is_recruited_new/is_allocated_to_school/
-- is_allocated_to_class) already derives only from this year's intake_applicants in
-- fct_volunteer_pipeline, so a volunteer with no 26-27 application contributes nothing here.
-- Stage 1 "Applied" = any non-draft status (is_applied OR is_completed) -- submitted something,
-- regardless of whether it's still pending or finished. assert_sric_funnel_intake_to_class_monotonic
-- checks the nesting invariant; a violation there is a real defect (unlike coverage's monotonicity
-- test, which exists to police a state-nesting ASSUMPTION, not a funnel).
--
-- FOURTH BLOCK, LEADS (2026-09-13): funnel_block = 'leads'. Replaces Dalgo's direct read of
-- fct_volunteer_pipeline for two national lead-attribution tiles -- the only reason Dalgo depended
-- on prod_gold_marts at all (dashboards are meant to read prod_gold_analytics exclusively).
-- National only (chapter_id NULL), 26-27 cohort (same intake scoping as above), stage_relationship
-- = 'exclusive' -- a volunteer is applied-and-not-yet-recruited via exactly one attribution path:
--   'Applied through a chapter''s link'  -- is_applied, not is_recruited_new, lead_attribution = 'chapter'
--   'Applied directly'                   -- is_applied, not is_recruited_new, lead_attribution <> 'chapter'
-- conversion_pct is not computed for this block (no prior stage to convert from -- these are two
-- parallel categories, not a sequence).
--
-- FIFTH BLOCK, VOLUNTEER SOURCE (2026-09-13): funnel_block = 'volunteer_source'. Per-chapter (not
-- national), scoped to chapters with classes_set_up = true. stage_relationship = 'exclusive' --
-- New this year and Returning from last year are the two non-overlapping parts of
-- volunteers_allocated_to_school:
--   'New this year'             -- volunteers_new_this_year
--   'Returning from last year'  -- volunteers_continuing
-- Sourced directly from prod_sric_dashboard_data's own composition split (already tested to sum
-- to volunteers_allocated_to_school) rather than re-deriving is_recruited_new logic against
-- fct_volunteer_pipeline a third time. conversion_pct is not computed -- two parallel categories,
-- not a sequence, same as the leads block above.
--
-- LAG PARTITION FIX (2026-09-12): with_conversion's window now partitions by (chapter_id,
-- funnel_block), not chapter_id alone. Previously, rows from different funnel_blocks with the same
-- chapter_id (including NULL, which SQL's PARTITION BY treats as one group) could tie on
-- stage_order and land adjacent in the same lag window in an undefined order -- harmless before
-- now (conversion_pct only ever read intake's stage 4), but load-bearing now that intake_to_class
-- needs a correct sequential lag at every stage. This only changes which row lag() looks at within
-- a tie; it does not change any existing conversion_pct value.

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
        is_ready_to_mentor,
        lead_attribution
    from {{ ref('fct_volunteer_pipeline') }}
),

-- chapter_status (2026-08-28): joined the same way chapter/city already are, from
-- prod_sric_dashboard_data -- not re-derived here, so this stays the one place chapter_status is
-- computed. NULL where chapter_id is null (unattributed volunteers, §6.8 -- see below): do not
-- coalesce this to false. An unattributed volunteer's chapter activity is unknown, not inactive,
-- and collapsing it to false would let a chart-level "active only" filter silently drop them
-- instead of showing them as their own group.
chapter_names as (
    select distinct
        chapter_id,
        chapter,
        city,
        chapter_status
    from {{ ref('prod_sric_dashboard_data') }}
),

-- Feeds the Needed reference row below -- one authoritative requirement per chapter, same source
-- as every other chapter-level figure in this model. Grain: one row per chapter_id.
chapter_requirements as (
    select distinct
        chapter_id,
        volunteers_required
    from {{ ref('prod_sric_dashboard_data') }}
),

intake_stage_dim as (
    select 1 as stage_order, '1 · Draft' as stage_name
    union all select 2, '2 · Applied'
    union all select 3, '3 · Application Complete'
    union all select 4, '4 · Recruited'
),

coverage_stage_dim as (
    select 1 as stage_order, '1 · Placed at a school' as stage_name
    union all select 2, '2 · Signed child safety policies'
    union all select 3, '3 · Attended induction'
    union all select 4, '4 · Placed in a class'
    union all select 5, '5 · Ready to mentor'
),

intake_to_class_stage_dim as (
    select 1 as stage_order, '1 · Applied' as stage_name
    union all select 2, '2 · Recruited'
    union all select 3, '3 · Placed at a school'
    union all select 4, '4 · Placed in a class'
),

leads_stage_dim as (
    select 1 as stage_order, 'Applied through a chapter''s link' as stage_name
    union all select 2, 'Applied directly'
),

volunteer_source_stage_dim as (
    select 1 as stage_order, 'New this year' as stage_name
    union all select 2, 'Returning from last year'
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

-- Needed reference row (see header, 2026-09-11): one per coverage chapter, stage_order 0,
-- volunteer_source NULL. Sourced from coverage_chapters (not chapter_requirements alone) so this
-- row only appears for chapters already present in the coverage block -- same dense-but-not-wider
-- rule the rest of this model follows.
needed_row as (
    select
        'coverage' as funnel_block,
        0 as stage_order,
        '0 · Needed' as stage_name,
        cc.chapter_id,
        cast(null as text) as volunteer_source,
        coalesce(cr.volunteers_required, 0) as volunteers
    from coverage_chapters cc
    left join chapter_requirements cr
        on cc.chapter_id = cr.chapter_id
),

-- Lead funnel (see header, 2026-09-12): national only, chapter_id NULL, 26-27 cohort. This
-- genuinely nests (each stage is a strict AND-superset of the one before), unlike intake/coverage.
intake_to_class_counts as (
    select
        'intake_to_class' as funnel_block,
        d.stage_order,
        d.stage_name,
        cast(null as text) as chapter_id,
        cast(null as text) as volunteer_source,
        count(distinct b.volunteer_id) as volunteers
    from intake_to_class_stage_dim d
    left join base b
        on (
            (d.stage_order = 1 and (b.is_applied or b.is_completed))
            or (d.stage_order = 2 and b.is_recruited_new)
            or (d.stage_order = 3 and b.is_recruited_new and b.is_allocated_to_school)
            or (d.stage_order = 4 and b.is_recruited_new and b.is_allocated_to_class)
        )
    group by d.stage_order, d.stage_name
),

-- National lead tiles (2026-09-13): replaces Dalgo's direct read of fct_volunteer_pipeline for
-- the two national lead-attribution tiles -- national only (chapter_id NULL), 26-27 cohort
-- (inherits the scoping already on is_applied/lead_attribution in fct_volunteer_pipeline).
-- stage_relationship = 'exclusive': a volunteer is applied-not-recruited via exactly one
-- attribution path (chapter-link vs. everything else), same status-distribution shape as intake.
leads_counts as (
    select
        'leads' as funnel_block,
        d.stage_order,
        d.stage_name,
        cast(null as text) as chapter_id,
        cast(null as text) as volunteer_source,
        count(distinct b.volunteer_id) as volunteers
    from leads_stage_dim d
    left join base b
        on b.is_applied and not b.is_recruited_new
        and (
            (d.stage_order = 1 and b.lead_attribution = 'chapter')
            or (d.stage_order = 2 and b.lead_attribution <> 'chapter')
        )
    group by d.stage_order, d.stage_name
),

-- Chapters scoped for the block below (2026-09-13): classes ready (classes_set_up = true) --
-- deliberately not also gated on chapter_status, unlike prod_sric_national_display's stricter
-- scope; a chapter can have classes running without being marked "active this year" on the sheet.
volunteer_source_chapters as (
    select distinct chapter_id, volunteers_new_this_year, volunteers_continuing
    from {{ ref('prod_sric_dashboard_data') }}
    where classes_set_up = true
),

-- Volunteer composition by chapter (2026-09-13): stage_relationship = 'exclusive' -- New this
-- year and Returning from last year are the two non-overlapping parts of
-- volunteers_allocated_to_school (tested to sum to it exactly in prod_sric_dashboard_data), same
-- status-distribution shape as intake/leads. Sourced directly from prod_sric_dashboard_data's
-- already-tested composition split rather than re-deriving is_recruited_new logic a third time.
volunteer_source_counts as (
    select
        'volunteer_source' as funnel_block,
        d.stage_order,
        d.stage_name,
        c.chapter_id,
        cast(null as text) as volunteer_source,
        case
            when d.stage_order = 1 then c.volunteers_new_this_year
            when d.stage_order = 2 then c.volunteers_continuing
        end as volunteers
    from volunteer_source_chapters c
    cross join volunteer_source_stage_dim d
),

all_stages as (
    select * from intake_counts
    union all
    select * from coverage_counts
    union all
    select * from needed_row
    union all
    select * from intake_to_class_counts
    union all
    select * from leads_counts
    union all
    select * from volunteer_source_counts
),

with_conversion as (
    select
        a.*,
        lag(a.volunteers) over (
            partition by a.chapter_id, a.funnel_block order by a.stage_order
        ) as prev_stage_volunteers
    from all_stages a
)

select
    a.funnel_block,
    case
        when a.funnel_block in ('intake', 'leads', 'volunteer_source') then 'exclusive'
        when a.funnel_block = 'intake_to_class' then 'nested'
        else 'overlapping'
    end as stage_relationship,
    a.stage_order,
    a.stage_name,
    a.chapter_id,
    cn.chapter,
    cn.city,
    cn.chapter_status,
    a.volunteer_source,
    a.volunteers,
    -- Completed -> Recruited (intake stage_order 4) is intake's one genuine progression -- see
    -- header. intake_to_class nests at every stage, so its conversion_pct is computed throughout
    -- (stage 1 is naturally NULL -- there's no stage before it to convert from).
    case
        when a.funnel_block = 'intake'
             and a.stage_order = 4
             and a.prev_stage_volunteers is not null
             and a.prev_stage_volunteers > 0
        then round(100.0 * a.volunteers / a.prev_stage_volunteers, 1)
        when a.funnel_block = 'intake_to_class'
             and a.prev_stage_volunteers is not null
             and a.prev_stage_volunteers > 0
        then round(100.0 * a.volunteers / a.prev_stage_volunteers, 1)
    end as conversion_pct
from with_conversion a
left join chapter_names cn
    on a.chapter_id = cn.chapter_id
