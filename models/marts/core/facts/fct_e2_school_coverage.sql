{{ config(materialized='table') }}

-- fct_e2_school_coverage: chapter-level mentor/volunteer coverage gaps for one academic year
-- Grain: one row per (chapter_id, academic_year)
-- Ported from the children_by_school_ay CTE in the legacy fct_dashboard_summary model.
-- total_sections/total_volunteers_assigned added as coverage denominators: total_sections counts
-- every class_section tied to the school's academic_year via the school_academic_year_id FK chain
-- (no is_active/is_removed archived-row guessing needed once that FK is resolved); total_volunteers_assigned
-- walks slot (via its school_academic_year_id) -> slot_class_section -> slot_class_section_volunteer
-- directly, rather than going through fct_e2_volunteer_allocation_history (which also requires the
-- parent slot_class_section to be non-removed -- a stricter condition than asked for here).
-- is_chapter_active/total_slots/total_classes/total_volunteers_assigned all branch on
-- dim_school_academic_year_status: for the year a school is currently active in, count the live
-- (is_active=true, is_removed=false) rows; for a year that's since been archived (is_ay_active=false),
-- Bubble keeps that year's rows around with is_active flipped to false rather than removing them, so
-- counting is_active=false, is_removed=false there gives the preserved historical footprint instead
-- of zero.
-- total_children_in_system is sourced from int_bubble__child_class (child -> school_class directly),
-- NOT from child_class_section -> class_section like total_children_with_mentor/without_mentor still
-- are. Two reasons: (1) child_class is the more fundamental enrollment -- mandatory at enrollment time,
-- present even before a child is ever assigned a specific class_section, so it reaches children the
-- class_section chain misses entirely (confirmed: the class_section chain drops 50 of 119 chapters'
-- worth of children for 2025-2026, mostly from schools whose school_academic_year or school_class
-- records are formally removed rather than just archived); (2) unlike slot/school_class,
-- child_class's is_active flag does NOT reliably flip to false once a year archives -- 29% of
-- archived-year child_class rows are still is_active=true -- so it's filtered on is_removed=false only,
-- with no active/archived branching, to avoid undercounting. This means total_children_in_system and
-- (total_children_with_mentor + children_without_mentor) are sourced from two different populations and
-- will not always sum to the same total -- that's expected, not a bug, given the two source tables
-- disagree on which children exist at all.
-- Also joined to int_bubble__children and filtered on is_active=true there (2026-08-13) -- a separate
-- signal from child_class.is_active: it's the child's own exit status (did this kid leave the program),
-- not tied to a specific year's archival state. Confirmed 2,061 children warehouse-wide have
-- is_active=false on their own record but a stale is_active=true child_class row -- e.g. 111 of the 177
-- children counted here for chapter 220 in 2026-2027 with no class_section assignment turned out to be
-- exactly this: exited children never cleaned up. Deliberately did NOT also filter
-- child_class.is_active=true itself -- tested it and it collapses 2025-2026 from 3,327 to 244 children,
-- which is the exact undercounting the paragraph above warns about, not a fix.
-- The same is_active=true filter (via int_bubble__children) is also applied to total_children_with_mentor/
-- children_without_mentor below, for the same reason: without it, an exited child who still has a
-- child_class_section assignment would be counted on the mentor side but not in total_children_in_system,
-- which had started producing a negative gap (mentor+non-mentor totals exceeding total_children_in_system
-- for 2025-2026) once total_children_in_system alone got this filter.

with class_sections_with_slot as (
    select distinct class_section_id
    from {{ ref('int_bubble__slot_class_section') }}
    where is_removed = false
),

school_class_sections as (
    select
        ay.label as academic_year,
        p.partner_id::text as chapter_id,
        cs.class_section_id
    from {{ ref('int_bubble__academic_year') }} ay
    join {{ ref('int_bubble__school_academic_year') }} say
        on ay.academic_year_id = say.academic_year_id
        and say.is_removed = false
    join {{ ref('int_bubble__school_class') }} scl
        on say.school_academic_year_id = scl.school_academic_year_id
        and say.school_id = scl.school_id
        and scl.is_removed = false
    join {{ ref('int_bubble__class_section') }} cs
        on scl.school_class_id = cs.school_class_id
        and cs.is_removed = false
    join {{ ref('int_bubble__partner') }} p
        on say.school_id = p.partner_id
),

volunteers_assigned as (
    select
        p.partner_id::text as chapter_id,
        ay.label as academic_year,
        count(distinct scsv.volunteer_id) filter (
            where (st.is_ay_active and scsv.is_active = true and scsv.is_removed = false)
               or (not st.is_ay_active and scsv.is_active = false and scsv.is_removed = false)
        ) as total_volunteers_assigned
    from {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    join {{ ref('int_bubble__slot_class_section') }} scs2v
        on scsv.slot_class_section_id = scs2v.slot_class_section_id
    join {{ ref('int_bubble__slot') }} s
        on scs2v.slot_id = s.slot_id
    join {{ ref('int_bubble__school_academic_year') }} say
        on s.school_academic_year_id = say.school_academic_year_id
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    join {{ ref('int_bubble__partner') }} p
        on say.school_id = p.partner_id
    left join {{ ref('dim_school_academic_year_status') }} st
        on say.school_id = st.school_id
        and ay.label = st.academic_year
    group by p.partner_id::text, ay.label
),

slot_counts as (
    select
        p.partner_id::text as chapter_id,
        ay.label as academic_year,
        count(distinct s.slot_id) filter (
            where (st.is_ay_active and s.is_active = true and s.is_removed = false)
               or (not st.is_ay_active and s.is_active = false and s.is_removed = false)
        ) as total_slots
    from {{ ref('int_bubble__slot') }} s
    join {{ ref('int_bubble__school_academic_year') }} say
        on s.school_academic_year_id = say.school_academic_year_id
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    join {{ ref('int_bubble__partner') }} p
        on say.school_id = p.partner_id
    left join {{ ref('dim_school_academic_year_status') }} st
        on say.school_id = st.school_id
        and ay.label = st.academic_year
    group by p.partner_id::text, ay.label
),

class_counts as (
    select
        p.partner_id::text as chapter_id,
        scs2.academic_year,
        count(distinct scs2.slot_class_section_id) filter (
            where (st.is_ay_active and scs2.is_active = true and scs2.is_removed = false)
               or (not st.is_ay_active and scs2.is_active = false and scs2.is_removed = false)
        ) as total_classes
    from {{ ref('int_bubble__slot_class_section') }} scs2
    join {{ ref('int_bubble__class_section') }} cs2
        on scs2.class_section_id = cs2.class_section_id
    join {{ ref('int_bubble__partner') }} p
        on cs2.school_id = p.partner_id
    left join {{ ref('dim_school_academic_year_status') }} st
        on cs2.school_id = st.school_id
        and scs2.academic_year = st.academic_year
    group by p.partner_id::text, scs2.academic_year
),

chapter_school_classes as (
    select
        p.partner_id::text as chapter_id,
        ay.label as academic_year,
        scl.school_class_id
    from {{ ref('int_bubble__school_class') }} scl
    join {{ ref('int_bubble__school_academic_year') }} say
        on scl.school_academic_year_id = say.school_academic_year_id
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    join {{ ref('int_bubble__partner') }} p
        on say.school_id = p.partner_id
    where scl.is_removed = false
),

children_in_system as (
    select
        csc.chapter_id,
        csc.academic_year,
        count(distinct cc.child_id) as total_children_in_system
    from chapter_school_classes csc
    join {{ ref('int_bubble__child_class') }} cc
        on csc.school_class_id = cc.school_class_id
        and cc.is_removed = false
    join {{ ref('int_bubble__children') }} ch
        on cc.child_id = ch.child_id
        and ch.is_active = true
        and ch.is_removed = false
    group by csc.chapter_id, csc.academic_year
),

-- Pre-aggregated so the final select can be plain left joins: section-level metrics still come from
-- the class_section chain (mentor coverage needs section-level granularity that child_class can't
-- provide), but the row set as a whole must not be limited to chapters that have a class_section --
-- see all_chapter_academic_years below.
section_level_metrics as (
    select
        scs.chapter_id,
        scs.academic_year,
        count(distinct scs.class_section_id) as total_sections,
        count(distinct ccs.child_id) filter (
            where cws.class_section_id is not null and ch.is_active = true and ch.is_removed = false
        ) as total_children_with_mentor,
        count(distinct ccs.child_id) filter (
            where cws.class_section_id is null and ch.is_active = true and ch.is_removed = false
        ) as children_without_mentor,
        count(distinct scs.class_section_id) filter (where cws.class_section_id is null) as sections_without_volunteer
    from school_class_sections scs
    left join {{ ref('int_bubble__child_class_section') }} ccs
        on scs.class_section_id = ccs.class_section_id
        and ccs.is_removed = false
    left join {{ ref('int_bubble__children') }} ch
        on ccs.child_id = ch.child_id
    left join class_sections_with_slot cws
        on scs.class_section_id = cws.class_section_id
    group by scs.chapter_id, scs.academic_year
),

-- The row set: a chapter/year must appear here even if it has school_classes but zero class_sections
-- (which is exactly the gap that made total_children_in_system come back null for 50 of 119 chapters
-- for 2025-2026) -- so this unions both populations rather than anchoring on just one.
all_chapter_academic_years as (
    select chapter_id, academic_year from school_class_sections
    union
    select chapter_id, academic_year from chapter_school_classes
)

select
    acay.chapter_id,
    acay.academic_year,
    st.is_ay_active as is_chapter_active,
    slm.total_sections,
    slc.total_slots,
    clc.total_classes,
    cis.total_children_in_system,
    slm.total_children_with_mentor,
    slm.children_without_mentor,
    slm.sections_without_volunteer,
    vas.total_volunteers_assigned
from all_chapter_academic_years acay
left join section_level_metrics slm
    on acay.chapter_id = slm.chapter_id
    and acay.academic_year = slm.academic_year
left join children_in_system cis
    on acay.chapter_id = cis.chapter_id
    and acay.academic_year = cis.academic_year
left join volunteers_assigned vas
    on acay.chapter_id = vas.chapter_id
    and acay.academic_year = vas.academic_year
left join slot_counts slc
    on acay.chapter_id = slc.chapter_id
    and acay.academic_year = slc.academic_year
left join class_counts clc
    on acay.chapter_id = clc.chapter_id
    and acay.academic_year = clc.academic_year
left join {{ ref('dim_school_academic_year_status') }} st
    on acay.chapter_id = st.school_id::text
    and acay.academic_year = st.academic_year
