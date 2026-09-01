{{ config(materialized='table') }}

-- fct_volunteer_chapter_intake: a hired volunteer, the day they were hired, and the chapter that
-- claimed them -- the two halves of "chapter X recruited a volunteer on day Y", which live in two
-- different systems and arrive at two different times.
--
-- Grain: one row per volunteer per academic year (the year comes from the intake opportunity via
-- seed_pc_opportunity_academic_year, per SRI_DASHBOARD_SPEC.md §12c rule 3 -- never parsed from
-- an opportunity name, and never derived from a date).
--
-- Flow: stg_pc_opportunity_applicant (hire event + date) + int_bubble__school_volunteer (chapter)
--       -> fct_volunteer_chapter_intake
--
-- WHY THIS FACT EXISTS. fct_volunteer_pipeline answers "where does this volunteer stand today" and
-- carries no dates at all -- deliberately, since it mixes a cohort flow with a current-state
-- census. Nothing in the project can currently answer "how many did this chapter recruit on
-- Tuesday", because the recruit event's date (PC) and its chapter attribution (Bubble) have never
-- been joined. Sibling fact rather than new columns on fct_volunteer_pipeline: different grain
-- question, and that model's grain is a public contract (§9 shared-contract rule).
--
-- THE TWO DATES, AND WHY BOTH ARE CARRIED.
--   hire_datetime  -- PC. When the volunteer was actually hired. Complete and reliable: all 593
--                     of this year's hires carry one, spanning 2026-04-12 to 2026-08-27.
--   mapped_date    -- Bubble. When some chapter attached that volunteer to a school, which is the
--                     only ID-resolvable chapter path (PC's worknode IDs have 0 matches against
--                     the mapping sheet -- see fct_volunteer_pipeline's KNOWN GAP).
--
-- Attribution is the binding constraint, and it lands late. Measured 2026-08-28 over the 100
-- attributable 26-27 hires: median lag hire -> mapping is 42 days, p90 is 82, max 118, and not one
-- volunteer was mapped before being hired. So a row's chapter_id is expected to be null on the day
-- it is created and to fill in weeks later. Consumers reading hire_date must treat recent days as
-- incomplete rather than as zero -- is_chapter_attributed exists so they can say which.
--
-- Coverage, same measurement: 593 hired, 137 have a school_volunteer row, 100 of those carry a
-- school_id. Unattributed hires are KEPT (chapter_id null, is_chapter_attributed false) rather
-- than filtered out -- they are the §6.11 "hired, not yet placed" population, and dropping them
-- would hide the campaign's own denominator.
--
-- Uses int_bubble__school_volunteer directly, NOT int_bubble__school_volunteer_backfilled. The
-- backfill infers a school from a live class assignment, which by construction only recovers
-- volunteers at chapters that have set up classes (~59 of 68 have not). For a leaderboard that
-- would be a systematic thumb on the scale in favour of the chapters that are already ahead. This
-- model wants the honest, directly-recorded link only.

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

-- The chapter that claimed the volunteer. Where a volunteer has been mapped to more than one
-- school, the EARLIEST non-removed mapping wins -- first chapter to claim them. Stated as a rule
-- rather than left to an arbitrary tiebreak because on a leaderboard it decides who scores.
mapping as (
    select distinct on (volunteer_id)
        volunteer_id::numeric as volunteer_id,
        school_id as chapter_id,
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
    m.chapter_id,
    m.mapped_date,
    (m.chapter_id is not null) as is_chapter_attributed,
    -- Kept as a measure rather than recomputed downstream: this is the number that decides
    -- whether a day-grain board can be read at all on the day it refreshes.
    (m.mapped_date - h.hire_datetime::date) as days_hire_to_mapping
from hires h
left join mapping m
    on h.volunteer_id = m.volunteer_id
