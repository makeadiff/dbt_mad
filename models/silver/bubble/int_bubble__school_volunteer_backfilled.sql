{{ config(materialized='table') }}

-- Resolves null school_id on school_volunteer via the live class-assignment chain.
-- Flow: int_bubble__school_volunteer + int_bubble__slot_class_section_volunteer ->
--       int_bubble__school_volunteer_backfilled
-- Consolidated from three near-identical copies (int_bubble__school_volunteer_metrics,
-- prod_sric_dashboard_data, fct_volunteer_pipeline) that had each implemented this backfill
-- independently -- a future change to the tiebreak or filter had to be made in three places to
-- stay consistent. This is now the one place it's defined; all three now reference it.
--
-- What it does: where school_volunteer.school_id is null, infers the school from the volunteer's
-- most recent currently-active class assignment (int_bubble__slot_class_section_volunteer ->
-- slot_class_section -> dim_class_section), on the reasoning that a volunteer actively assigned
-- to a section at school X is, in fact, at school X.
--
-- KNOWN BIAS (SRI_DASHBOARD_SPEC.md §6.3): this only recovers volunteers who have a live class
-- assignment. It systematically undercounts schools that have recruited volunteers but no
-- classes set up -- the exact population this dashboard exists to flag. ~148 volunteers
-- project-wide are recoverable this way; the great majority of null-school rows have no class
-- assignment either and stay unattributable, honestly, rather than guessed at.
--
-- EXPIRY (SRI_DASHBOARD_SPEC.md §6.8): this model is a workaround, not permanent architecture.
-- 95% of currently-active school_volunteer rows have both school_id AND academic_year null
-- together -- a creation path that writes neither, not people forgetting to pick a school.
-- That is a platform-side defect awaiting escalation to the Bubble/platform owner, not a dbt
-- problem. Once it's fixed at the source, this model's join should stop finding anything to
-- backfill, and the model -- along with the three consumers pointed at it -- is expected to be
-- deleted, with consumers reverting to fct_school_volunteer directly.
--
-- Grain: one row per (volunteer_id, academic_year), matching fct_school_volunteer -- currently
-- non-removed school_volunteer rows only (is_removed = false), since every consumer today wants
-- the "currently recruited" population, not the full history.
--
-- GRAIN IS ENFORCED, NOT ASSUMED (2026-08-28): this grain was declared above from the start but
-- never actually deduped -- school_volunteer has no field that marks an old assignment
-- superseded (confirmed at every layer, raw bubble_raw.school_volunteer included), so a volunteer
-- re-added to a second school without the first row being removed produced two live rows here,
-- both is_active = true / is_removed = false, silently violating the declared grain. Found via
-- volunteer 2125787: added to school 549 (chapter "Brilliant Academy - B2") on 2026-08-27 without
-- its 2026-06-16 row at school 550 ("Brilliant Academy - B1", the same physical school under a
-- second school record) ever being removed. This fanned out into every one of this model's three
-- consumers independently (int_bubble__school_volunteer_metrics counting him at both schools,
-- fct_volunteer_pipeline and prod_sric_dashboard_data's own backfilled join doing the same) --
-- exactly the kind of divergence this consolidation was meant to prevent, one layer down.
-- Reduced to one row per (volunteer_id, academic_year): most recent created_date, then highest
-- school_id as a stable tiebreak. Verified against the Bubble frontend, which shows volunteer
-- 2125787 on B2 (school 549, the newer row) only -- matching this tiebreak's pick. is_multi_school
-- flags every volunteer this reduction actually changes, so the case stays visible to consumers
-- rather than silently resolved.

with recruited as (
    select
        school_volunteer_id,
        volunteer_id,
        academic_year,
        school_id,
        created_date
    from {{ ref('int_bubble__school_volunteer') }}
    where is_removed = false
),

backfilled as (
    select
        r.school_volunteer_id,
        r.volunteer_id,
        r.academic_year,
        coalesce(r.school_id, backfill.school_id) as school_id,
        r.created_date
    from recruited r
    left join lateral (
        select cs.school_id
        from {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
        inner join {{ ref('int_bubble__slot_class_section') }} scs
            on scsv.slot_class_section_id = scs.slot_class_section_id
        inner join {{ ref('dim_class_section') }} cs
            on scs.class_section_id = cs.class_section_id
        where scsv.volunteer_id = r.volunteer_id
          and scsv.is_removed = false
          and scsv.is_active = true
        order by scsv.modified_date desc
        limit 1
    ) backfill on r.school_id is null
),

multi_school as (
    select volunteer_id, academic_year
    from backfilled
    group by volunteer_id, academic_year
    having count(distinct school_id) > 1
),

ranked as (
    select
        b.school_volunteer_id,
        b.volunteer_id,
        b.academic_year,
        b.school_id,
        (ms.volunteer_id is not null) as is_multi_school,
        row_number() over (
            partition by b.volunteer_id, b.academic_year
            order by b.created_date desc, b.school_id desc
        ) as rn
    from backfilled b
    left join multi_school ms
        on b.volunteer_id = ms.volunteer_id
        and b.academic_year is not distinct from ms.academic_year
)

select
    school_volunteer_id,
    volunteer_id,
    academic_year,
    school_id,
    is_multi_school
from ranked
where rn = 1
