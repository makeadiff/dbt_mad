{{ config(materialized='table') }}

-- fct_volunteer_pipeline: the SRIC dashboard's Row 2 funnel data, split into the two objects
-- signed in SRI_DASHBOARD_SPEC.md §12c -- INTAKE (this year's applicant cohort, PC-side) and
-- COVERAGE (everyone currently active, any year, Bubble-side). They are two different
-- populations answering two different questions and must never be converted into each other
-- (doing so produces percentages above 100%, the same error class as the original D1 405 > 184).
--
-- Grain: one row per volunteer, for the current reporting academic year (2026-27, from
-- seed_pc_opportunity_academic_year). This build does not attempt historical multi-year rows --
-- INTAKE columns are scoped to the single 26-27 volunteer-intake opportunity (id 5246, per the
-- seed); COVERAGE columns are a current-state snapshot regardless of when the volunteer joined.
-- Re-running this model for a past year would need a different COVERAGE snapshot mechanism, not
-- built here.
--
-- GRAIN IS ENFORCED, NOT ASSUMED (2026-08-28): a volunteer can be re-added to a second
-- chapter/school without the first assignment ever being removed (school_volunteer has no field
-- that marks the old one superseded). That's now resolved once, upstream, in
-- int_bubble__school_volunteer_backfilled (see its header) rather than here -- this model's
-- mapped_by_volunteer just passes through that resolution's is_multi_chapter flag.
-- allocated_by_volunteer has its own local dedup for the same defect class on a different source
-- table (slot_class_section_volunteer, not shared with another "allocated" consumer today).
--
-- Scope: E2 chapters only, matching prod_sric_dashboard_data's WHERE engine = 'E2'.
--
-- KNOWN GAP -- chapter_id on the INTAKE side (§12c): PC's own worknode ID space does not match
-- the mapping sheet's worknode_id (confirmed 0 matches on ID and on name, 2026-08-26) -- a
-- MAD_CHAPTER-scoped applicant cannot be resolved to a specific chapter_id via worknode alone.
-- The only ID-resolvable chapter path is sourcing_campaign_code. Applicants who are chapter-scoped
-- by worknode but have no campaign-code match (~413 of the 26-27 cohort, per 2026-08-26 profiling)
-- get lead_attribution = 'chapter_unmatched' (§12d) rather than 'chapter' or 'open_pool' -- we
-- know they intend a chapter, just not which one, and that must stay visible rather than being
-- silently folded into the national open pool.
--
-- city_name (not city_id): no dimension exists that resolves a city consistently across both
-- Bubble and Platform Commons -- PC's CITY-type worknode carries a name, not an id that joins to
-- anything on the Bubble side. Using the resolved name directly rather than inventing an id with
-- nothing to key against.

with intake_opportunity as (
    select opportunity_id
    from {{ ref('seed_pc_opportunity_academic_year') }}
    where academic_year = '2026-27' and is_volunteer_intake = true
),

e2_chapter_lookup as (
    select
        m.chapter_id,
        p.bubble_partner_id as school_id,
        m.sourcing_campaign_code
    from {{ ref('int_google_sheet__chapter_mapping') }} m
    inner join {{ ref('dim_bubble_partner') }} p
        on m.chapter_id::integer = p.bubble_partner_id::integer
    where m.engine = 'E2'
),

-- INTAKE: this year's cohort only, scoped to the single volunteer-intake opportunity (rule 1,
-- §12c) -- never applied to allocation/onboarding/compliance/coverage below.
intake_applicants as (
    select
        a.user_id::numeric as volunteer_id,
        {{ clean_prefix('a.application_status') }} as application_status,
        {{ clean_prefix('a.current_step_status') }} as current_step_status,
        a.applicant_campaign,
        w.worknode_type,
        w.worknode_name,
        cm.chapter_id as campaign_chapter_id
    from {{ ref('stg_pc_opportunity_applicant') }} a
    inner join intake_opportunity io
        on a.opportunity_id = io.opportunity_id
    left join {{ ref('stg_pc_worknode') }} w
        on a.applied_to_entity_id = w.worknode_id
    left join e2_chapter_lookup cm
        on a.applicant_campaign = cm.sourcing_campaign_code
    where a.is_deleted = false
      and a.user_id is not null
),

intake_by_volunteer as (
    -- Business mappings, REVISED 2026-08-27: Draft/Applied/Completed are the three
    -- non-exit values of the single ApplicationStatus field (DRAFT/PENDING/COMPLETED/WITHDRAWN),
    -- so they are exclusive current-status flags, not cumulative ones -- is_applied was
    -- previously `IN ('PENDING', 'COMPLETED')`, which made every Completed volunteer also count
    -- as Applied (590 of 590 completed volunteers, confirmed 2026-08-27: zero other overlap).
    -- is_applied is now PENDING only, matching the field 1:1. is_recruited_new (CurrentStepStatus
    -- 'HIRE') is a genuinely separate field and is NOT exclusive with is_completed --
    -- Completed > Recruited is expected (an application can finish without a hire).
    select
        volunteer_id,
        bool_or(application_status = 'DRAFT') as is_draft,
        bool_or(application_status = 'PENDING') as is_applied,
        bool_or(application_status = 'COMPLETED') as is_completed,
        bool_or(current_step_status = 'HIRE') as is_recruited_new,
        -- lead_attribution (§12c, revised §12d): chapter (campaign-code match, the only
        -- ID-resolvable path) > chapter_unmatched (worknode says MAD_CHAPTER but we can't say
        -- which one -- see KNOWN GAP above; kept distinct from open_pool per §12d so it isn't
        -- silently folded into the national pool) > city (CITY-type worknode) > open_pool.
        case
            when bool_or(campaign_chapter_id is not null) then 'chapter'
            when bool_or(worknode_type ilike '%MAD_CHAPTER%') then 'chapter_unmatched'
            when bool_or(worknode_type ilike '%CITY%') then 'city'
            else 'open_pool'
        end as lead_attribution,
        max(campaign_chapter_id) as intake_chapter_id,
        max(worknode_name) filter (where worknode_type ilike '%CITY%') as intake_city_name
    from intake_applicants
    group by volunteer_id
),

-- COVERAGE: current-state census, no opportunity filter, any year -- §12c rule 1.
-- school_id backfill (null school_id -> live class assignment) lives in
-- int_bubble__school_volunteer_backfilled -- see that model's header for the bias and its expiry
-- (a workaround for §6.8, expected to be deleted once fixed upstream).
--
-- ONE ROW PER VOLUNTEER, 2026-08-28: int_bubble__school_volunteer_backfilled enforces its own
-- declared grain (one row per volunteer_id, academic_year) as of 2026-08-28 -- see that model's
-- header for why it didn't before and the volunteer (2125787) that surfaced it. Because school_id
-- resolves 1:1 to chapter_id for E2 chapters, that upstream fix means this join can no longer fan
-- out a volunteer across chapters either -- fixed once, upstream, rather than re-deduping here
-- (two code paths computing "volunteers allocated to school" is exactly how they diverged from
-- int_bubble__school_volunteer_metrics in the first place). is_multi_chapter passes through
-- backfilled's is_multi_school flag so the case stays visible here too.
mapped_by_volunteer as (
    select distinct
        mv.volunteer_id,
        ecl.chapter_id,
        mv.is_multi_school as is_multi_chapter
    from {{ ref('int_bubble__school_volunteer_backfilled') }} mv
    inner join e2_chapter_lookup ecl
        on mv.school_id = ecl.school_id
),

-- Allocated (§2): currently allocated, matching D1(a) -- scsv.is_active = true, not just
-- is_removed = false.
--
-- DEDUPED 2026-08-28, same defect class as mapped_by_volunteer above: a volunteer with a live
-- class assignment in more than one chapter would fan out the same way. Reduced the same way
-- (most recent created_date, then highest chapter_id as tiebreak) -- chapter_id is resolved via
-- school_id here purely to make the tiebreak deterministic; this CTE's own population is
-- otherwise unchanged (still every currently-active class assignment, not scoped to E2 chapters).
allocated_by_volunteer_raw as (
    select distinct
        scsv.volunteer_id,
        cs.school_id,
        ecl.chapter_id,
        scsv.created_date
    from {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    inner join {{ ref('int_bubble__slot_class_section') }} scs
        on scsv.slot_class_section_id = scs.slot_class_section_id
    inner join {{ ref('dim_class_section') }} cs
        on scs.class_section_id = cs.class_section_id
    left join e2_chapter_lookup ecl
        on cs.school_id = ecl.school_id
    where scsv.is_removed = false and scs.is_removed = false and scsv.is_active = true
),

allocated_by_volunteer as (
    select volunteer_id, school_id
    from (
        select
            volunteer_id,
            school_id,
            row_number() over (
                partition by volunteer_id
                order by created_date desc, chapter_id desc nulls last
            ) as rn
        from allocated_by_volunteer_raw
    ) ranked
    where rn = 1
),

-- Compliance (§2.1): CPP + COC accepted, in ANY application (fixed 2026-08-27 -- see
-- int_pc_applicant_policy_status's header; the old dedup-to-latest logic discarded real
-- acceptances). Gate on "can this volunteer start class" -- separate from onboarding, never
-- merged. compliance_year is the year of the most recent compliant application -- an
-- approximation from the application's own timestamp, not a signature date (see that model's
-- header) -- carried through so a consumer can filter to "compliant this year" without a
-- rebuild; this model does not decide whether that filter should apply.
compliance_by_volunteer as (
    select
        user_id::numeric as volunteer_id,
        is_compliant,
        compliance_year
    from {{ ref('int_pc_applicant_policy_status') }}
),

-- Induction (§2.2, business mappings signed 2026-08-26): event_type = 'ONBOARDING' only
-- (RECRUITMENT_DRIVE is the recruitment workshop, not induction), attendance_status = 'COMPLETED'
-- (not a boolean -- profiling found it's a status string).
induction_by_volunteer as (
    select distinct volunteer_id::numeric as volunteer_id
    from {{ ref('fct_pc_events') }}
    where event_type = 'ONBOARDING'
      and attendance_status = 'COMPLETED'
),

combined as (
    select
        coalesce(i.volunteer_id, m.volunteer_id, al.volunteer_id) as volunteer_id
    from intake_by_volunteer i
    full outer join mapped_by_volunteer m on i.volunteer_id = m.volunteer_id
    full outer join allocated_by_volunteer al on coalesce(i.volunteer_id, m.volunteer_id) = al.volunteer_id
)

select
    '2026-27' as academic_year,
    c.volunteer_id,
    coalesce(m.chapter_id, i.intake_chapter_id) as chapter_id,
    i.intake_city_name as city_name,
    i.lead_attribution,

    -- INTAKE (26-27 cohort only)
    coalesce(i.is_draft, false) as is_draft,
    coalesce(i.is_applied, false) as is_applied,
    coalesce(i.is_completed, false) as is_completed,
    coalesce(i.is_recruited_new, false) as is_recruited_new,

    -- COVERAGE (current state, any year)
    (m.volunteer_id is not null) as is_allocated_to_school,
    -- True if the source had more than one chapter for this volunteer before the dedup upstream
    -- (int_bubble__school_volunteer_backfilled) picked one -- see mapped_by_volunteer's header.
    -- Kept visible rather than silently resolved.
    coalesce(m.is_multi_chapter, false) as is_multi_chapter,
    (al.volunteer_id is not null) as is_allocated_to_class,
    coalesce(comp.is_compliant, false) as is_compliant,
    comp.compliance_year,
    (coalesce(comp.is_compliant, false) and ind.volunteer_id is not null) as is_onboarded,
    -- Renamed 2026-08-27: this is eligibility (allocated AND compliant AND inducted), not
    -- evidence of actually mentoring -- real sessions live in attendance data this model doesn't
    -- touch. "Active in class" overclaimed what's actually known.
    (
        al.volunteer_id is not null
        and coalesce(comp.is_compliant, false)
        and ind.volunteer_id is not null
    ) as is_ready_to_mentor

from combined c
left join intake_by_volunteer i on c.volunteer_id = i.volunteer_id
left join mapped_by_volunteer m on c.volunteer_id = m.volunteer_id
left join allocated_by_volunteer al on c.volunteer_id = al.volunteer_id
left join compliance_by_volunteer comp on c.volunteer_id = comp.volunteer_id
left join induction_by_volunteer ind on c.volunteer_id = ind.volunteer_id
