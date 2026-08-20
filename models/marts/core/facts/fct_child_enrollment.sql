{{ config(materialized='table') }}

-- fct_child_enrollment: a child's enrollment in a school_class, with an optional class_section refinement
-- Grain: one row per (child_id, school_class_id) enrollment
-- Built on int_bubble__child_class (the mandatory, base enrollment -- present even before, or without
-- ever, a class_section being created) and int_bubble__child_class_section (the more specific, optional
-- section assignment), matched on a shared school_class_id (resolved from class_section's own FK) so
-- the two tables can finally be joined on a real key instead of drifting as two independent facts.
-- Addresses SCHEMA_GAPS_AND_LEARNINGS.md #10: the two source tables disagree on who's enrolled (one
-- misses ~54% of what the other has, depending on which schools have gotten around to creating
-- class_sections), and nothing upstream marks one as authoritative or the other as a refinement of it.
-- This model is that refinement -- one enrollment record with a nullable class_section_id, the target
-- shape recommended for the new tool's actual schema, implemented here as a dbt-layer fix in the
-- meantime, not a substitute for fixing it at the source.
--
-- Both sides are filtered to is_removed = false (a removed enrollment/section-assignment is genuinely
-- gone, not a live fact worth surfacing) -- but is_active is deliberately left unfiltered and exposed
-- as a raw column, not baked into the grain: gap #1 documents child_class.is_active NOT reliably
-- flipping on archival (29% of archived-year rows stay is_active=true), so filtering on it here would
-- silently undercount the exact way fct_e2_school_coverage's header comment already warns against.
-- Leave that decision to each consumer, same as that model does.
--
-- has_class_record / has_section_record: matched via a FULL OUTER JOIN on (child_id, school_class_id),
-- not a plain LEFT JOIN from child_class -- a plain left join would silently drop any
-- child_class_section row whose child_class counterpart is missing (a data-quality orphan), which is
-- exactly the kind of silent drop this model exists to stop causing. has_class_record = false is a
-- live, queryable signal of that specific gap instead of something that has to be discovered by
-- cross-referencing two tables by hand.

with child_class_base as (
    select *
    from {{ ref('int_bubble__child_class') }}
    where is_removed = false
),

child_class_section_resolved as (
    -- resolve class_section -> its parent school_class_id so section assignments match the
    -- child_class row for the *same* class, not just the same child
    select
        ccs.child_class_section_id,
        ccs.child_id,
        ccs.class_section_id,
        cs.school_class_id,
        ccs.academic_year,
        ccs.created_date,
        ccs.modified_date
    from {{ ref('int_bubble__child_class_section') }} ccs
    join {{ ref('int_bubble__class_section') }} cs
        on ccs.class_section_id = cs.class_section_id
    where ccs.is_removed = false
),

enrollment_keys as (
    select distinct child_id, school_class_id from child_class_base
    union
    select distinct child_id, school_class_id from child_class_section_resolved
)

select
    ek.child_id,
    ek.school_class_id,
    coalesce(cc.academic_year, ccsr.academic_year) as academic_year,
    cc.child_class_id,
    cc.is_active as class_is_active,
    cc.created_date as class_created_date,
    cc.modified_date as class_modified_date,
    ccsr.child_class_section_id,
    ccsr.class_section_id,
    ccsr.created_date as section_created_date,
    ccsr.modified_date as section_modified_date,
    (cc.child_class_id is not null) as has_class_record,
    (ccsr.class_section_id is not null) as has_section_record
from enrollment_keys ek
left join child_class_base cc
    on ek.child_id = cc.child_id
    and ek.school_class_id = cc.school_class_id
left join child_class_section_resolved ccsr
    on ek.child_id = ccsr.child_id
    and ek.school_class_id = ccsr.school_class_id
