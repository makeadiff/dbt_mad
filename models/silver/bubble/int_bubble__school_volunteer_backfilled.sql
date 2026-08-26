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

with recruited as (
    select
        school_volunteer_id,
        volunteer_id,
        academic_year,
        school_id
    from {{ ref('int_bubble__school_volunteer') }}
    where is_removed = false
)

select
    r.school_volunteer_id,
    r.volunteer_id,
    r.academic_year,
    coalesce(r.school_id, backfill.school_id) as school_id
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
