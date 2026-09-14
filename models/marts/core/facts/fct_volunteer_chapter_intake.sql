{{ config(materialized='table') }}

-- fct_volunteer_chapter_intake: a hired volunteer, the day they were hired, and the chapter that
-- claimed them -- the two halves of "chapter X recruited a volunteer on day Y", which live in two
-- different systems and arrive at two different times.
--
-- Grain: one row per volunteer per academic year (the year comes from the intake opportunity via
-- seed_pc_opportunity_academic_year, per SRI_DASHBOARD_SPEC.md §12c rule 3 -- never parsed from
-- an opportunity name, and never derived from a date).
--
-- Flow: stg_pc_opportunity_applicant (hire event + date)
--       + stg_pc_workforce -> stg_pc_worknode (CENTER) -> mapping sheet worknode_id (chapter, 1st)
--       + int_bubble__school_volunteer (chapter, fallback)
--       -> fct_volunteer_chapter_intake
--
-- WHY THIS FACT EXISTS. fct_volunteer_pipeline answers "where does this volunteer stand today" and
-- carries no dates at all -- deliberately, since it mixes a cohort flow with a current-state
-- census. Nothing else in the project can answer "how many did this chapter recruit on Tuesday",
-- because the recruit event's date and its chapter attribution have never been joined. Sibling
-- fact rather than new columns on fct_volunteer_pipeline: different grain question, and that
-- model's grain is a public contract (§9 shared-contract rule).
--
-- ATTRIBUTION SOURCE, REWIRED per SRI_DASHBOARD_SPEC.md §6.15 (2026-09-15). Two sources, workforce
-- first, Bubble as fallback -- attribution_source records which one won:
--
--   1. WORKFORCE (attribution_source = 'workforce'). stg_pc_workforce (user_id -> worknode_id) is
--      set the moment a volunteer is placed in PC. §12c's original claim that PC worknode_id has
--      "0 matches against the mapping sheet" was a TYPE mistake, not a real ID mismatch: the sheet's
--      Worknode_ID points at WN_TYPE.CENTER worknodes (not the MAD_CHAPTER city-level nodes §12c
--      checked), and joined on that type, 113 of 113 sheet worknode IDs match, covering every
--      active E2 chapter (confirmed 2026-09-15; names agree, e.g. sheet "1 SV High School" <-> PC
--      "E2-1 SV High School"). Restricted to engine = 'E2' since workforce/worknode also covers E1.
--      Where a volunteer has more than one E2 CENTER workforce row, the EARLIEST by
--      modified_datetime (tiebreak: lowest workforce_id) wins -- same "first chapter to claim them"
--      rule the Bubble path already used, now stated once for both.
--   2. BUBBLE (attribution_source = 'bubble'). Fallback only, when no workforce row exists. Earliest
--      non-removed int_bubble__school_volunteer mapping, unchanged from before this rewrite.
--
-- COVERAGE AND AGREEMENT (measured 2026-09-15, 714 26-27 hires): workforce resolves 219, Bubble
-- resolves 177 -- and every Bubble-attributed volunteer is a SUBSET of the workforce-attributed set
-- (full containment: workforce already covers all 177 Bubble also covers). Of those 177, workforce
-- and Bubble agree on the chapter 167 times (94.4%) -- the other 5.6% is workforce winning a
-- placement Bubble hasn't caught yet, or the volunteer genuinely moved chapters between the two
-- events. Because Bubble's 177 is fully contained in workforce's 219, Bubble contributes ZERO rows
-- beyond what workforce already attributes -- attribution_source is never 'bubble' in practice
-- today, only kept as a real fallback for the day a volunteer is Bubble-mapped before being placed
-- in PC. Net effect: attributed count rises 177 -> ~219 (== workforce's own coverage, not additive).
--
-- CAMPAIGN LINK IS DELIBERATELY NOT A THIRD SOURCE. applicantCampaign -> sheet
-- Sourcing_Campaign_Code resolves a chapter for 253 of 714 hires -- more coverage than either source
-- above, and available immediately at application time. It is NOT used here because it records who
-- SOURCED the volunteer, not where they were PLACED, and the two disagree too often to put on a
-- leaderboard: it agrees with workforce on the chapter only 75% of the time (106/141) and with
-- Bubble 70%. Do not add it as a third fallback without re-deciding this -- see SRI_DASHBOARD_SPEC.md
-- §6.15 for the full investigation and Akshay's decision.
--
-- LAG, STILL REAL. Workforce placement doesn't make this a live feed -- it's a separate manual step
-- after hire, not an automatic one. Measured over the 219 workforce-attributed hires: median lag
-- hire -> workforce placement is 30 days (p90 81), using stg_pc_workforce.xModifiedTimestamp, which
-- is an UPPER BOUND on the true placement date (it moves on any edit to the row, not just the
-- worknode assignment). So a row's chapter_id is still expected to be null on the day it is
-- created and fill in over the following weeks -- is_chapter_attributed still exists so consumers
-- can say which. Unattributed hires are KEPT (chapter_id null) rather than filtered out -- they are
-- the §6.11 "hired, not yet placed" population, and dropping them would hide the campaign's own
-- denominator.
--
-- OPS RULE, NOT A MODEL CHANGE: this fact (and the campaign board built on it) only reads live if
-- recruiters set the worknode in PC at the moment of hire. That has to be communicated to the field
-- teams (Vijayawada and Ahmedabad specifically, per §6.15) -- without it, this rewrite improves
-- coverage and agreement but does not fix the lag, and the board stays a lagging record regardless
-- of which source it reads.
--
-- Uses int_bubble__school_volunteer directly for the Bubble fallback, NOT
-- int_bubble__school_volunteer_backfilled. The backfill infers a school from a live class
-- assignment, which by construction only recovers volunteers at chapters that have set up classes
-- (~59 of 68 have not). For a leaderboard that would be a systematic thumb on the scale in favour
-- of the chapters that are already ahead. This model wants the honest, directly-recorded link only.

with intake_opportunity as (
    select opportunity_id, academic_year
    from {{ ref('seed_pc_opportunity_academic_year') }}
    where is_volunteer_intake = true
),

-- One hire per volunteer per year. A volunteer can hold more than one applicant row against the
-- same opportunity; the earliest HIRE timestamp is the moment they were recruited.
hires as (
    select
        io.academic_year,
        a.user_id::numeric as volunteer_id,
        min(a.current_step_datetime) as hire_datetime
    from {{ ref('stg_pc_opportunity_applicant') }} a
    inner join intake_opportunity io
        on a.opportunity_id = io.opportunity_id
    where a.is_deleted = false
      and a.user_id is not null
      and {{ clean_prefix('a.current_step_status') }} = 'HIRE'
      and a.current_step_datetime is not null
    group by 1, 2
),

-- Primary source (§6.15): PC workforce placement, resolved to a chapter via the sheet's CENTER
-- worknode ID -- restricted to engine = 'E2' since the same worknode space also covers E1. Earliest
-- placement wins where a volunteer has more than one (see header).
workforce_mapping as (
    select distinct on (wf.user_id)
        wf.user_id::numeric as volunteer_id,
        s.chapter_id as chapter_id,
        wf.modified_datetime::date as mapped_date
    from {{ ref('stg_pc_workforce') }} wf
    inner join {{ ref('stg_pc_worknode') }} w
        on wf.worknode_id = w.worknode_id
       and w.worknode_type = 'WN_TYPE.CENTER'
    inner join {{ ref('stg_google_sheet__master_mapping_sheet') }} s
        on w.worknode_id = s.worknode_id
       and s.engine = 'E2'
    order by wf.user_id, wf.modified_datetime asc, wf.workforce_id asc
),

-- Fallback source: the chapter that claimed the volunteer in Bubble. Where a volunteer has been
-- mapped to more than one school, the EARLIEST non-removed mapping wins -- first chapter to claim
-- them, same rule workforce_mapping above uses.
bubble_mapping as (
    select distinct on (volunteer_id)
        volunteer_id::numeric as volunteer_id,
        school_id::text as chapter_id,
        created_date as mapped_date
    from {{ ref('int_bubble__school_volunteer') }}
    where is_removed = false
      and school_id is not null
      and volunteer_id is not null
    order by volunteer_id, created_date asc, school_volunteer_id asc
)

select
    h.academic_year,
    h.volunteer_id,
    h.hire_datetime,
    h.hire_datetime::date as hire_date,
    coalesce(wf.chapter_id, bm.chapter_id) as chapter_id,
    case
        when wf.chapter_id is not null then 'workforce'
        when bm.chapter_id is not null then 'bubble'
    end as attribution_source,
    coalesce(wf.mapped_date, bm.mapped_date) as mapped_date,
    (coalesce(wf.chapter_id, bm.chapter_id) is not null) as is_chapter_attributed,
    -- Kept as a measure rather than recomputed downstream: this is the number that decides
    -- whether a day-grain board can be read at all on the day it refreshes.
    (coalesce(wf.mapped_date, bm.mapped_date) - h.hire_datetime::date) as days_hire_to_mapping
from hires h
left join workforce_mapping wf
    on h.volunteer_id = wf.volunteer_id
left join bubble_mapping bm
    on h.volunteer_id = bm.volunteer_id
