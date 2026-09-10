{{ config(materialized='table') }}

-- fct_e1_school_coverage: school-level mentor/volunteer coverage gaps for one academic year, for
-- E1 (session-ops / platform_commons). E1 counterpart to fct_e2_school_coverage (Bubble/E2) -- same
-- shape and column set, adapted to platform_commons' batch-based model instead of Bubble's
-- class_section/slot_class_section chain.
-- Grain: one row per (school_id, academic_year)
-- Flow: int_pc_batch_coverage (batch -> slot_shift -> volunteer -> attendance) for the
--       sections/slots/classes/volunteer metrics, int_pc_child_batch_enrollment + stg_pc_student
--       for the children metrics -> fct_e1_school_coverage
--
-- Differences from fct_e2_school_coverage, both deliberate:
-- 1. is_school_active is a flat school.is_active (from int_pc_school_id), not a per-(school,year)
--    active/archived flag. E2's is_chapter_active comes from dim_school_academic_year_status, a
--    Bubble-only dimension that exists because Bubble preserves an archived year's rows with
--    is_active flipped to false rather than removing them. Platform_commons has no equivalent
--    per-year archival table, so there's no signal to build that branch on -- every batch/slot_shift/
--    volunteer tied to a given academic_year is counted here regardless of its current is_active
--    flag (see int_pc_batch_coverage's header for the same reasoning applied to its own counts).
-- 2. total_children_with_mentor/children_without_mentor split on whether the child's batch has an
--    ACTUAL tagged volunteer (owner_user_id present via the worknode_slot_shift_user bridge/list),
--    not merely "batch has a slot_shift assigned" the way E2's class_sections_with_slot proxy does.
--    Platform_commons exposes the tagged-volunteer join directly, so there's no need for the
--    slot-existence proxy E2 relies on in the absence of that data.
-- total_children_in_system filters on stg_pc_student.is_active (the student's own record --
-- analogous to E2's ch.is_active child exit-status check), NOT batch_student.is_active
-- (enrollment_active_status): the enrollment flag is scoped to one batch/year and would zero out
-- otherwise-real historical enrollment once a year's batches go inactive, which is exactly the
-- undercounting E2 already had to work around (see fct_e2_school_coverage's chapter_school_classes/
-- children_in_system notes) by not filtering on the year-scoped flag either.
-- total_volunteers_in_school is all-time (school-level only, NOT year-scoped) -- distinct
-- owner_user_id ever tagged to any of the school's slot_shifts, across every academic_year. The
-- same value repeats on every academic_year row for that school. This substitutes for E2's
-- total_volunteers_in_school (fct_e2_volunteer_recruitment), which counts a Bubble-only recruitment-
-- intake bucket table with no platform_commons equivalent; using all-time tagged-volunteer count
-- instead trades "everyone recruited to this school" for "everyone who ever actually taught a
-- class here," which is what's reliably knowable from platform_commons data.

with batch_coverage as (
    select * from {{ ref('int_pc_batch_coverage') }}
),

batch_agg as (
    select
        school_id,
        academic_year,
        count(distinct sc_level_batch_id) as total_sections,
        count(distinct worknode_slot_id) as total_slots,
        count(distinct worknode_slot_shift_id) as total_classes,
        count(distinct worknode_slot_shift_id) filter (where has_attendance) as classes_started,
        count(distinct worknode_slot_shift_id) filter (
            where not has_attendance and worknode_slot_shift_id is not null
        ) as classes_not_started,
        count(distinct owner_user_id) as total_volunteers_assigned
    from batch_coverage
    group by school_id, academic_year
),

-- classes_with_more_than_1_volunteer: slot_shifts with more than one distinct tagged volunteer --
-- single-volunteer slot_shifts are deliberately excluded, matching E2's
-- classes_with_multiple_volunteers semantics.
slot_shift_volunteer_counts as (
    select
        school_id,
        academic_year,
        worknode_slot_shift_id,
        count(distinct owner_user_id) as volunteer_count
    from batch_coverage
    where worknode_slot_shift_id is not null
    group by school_id, academic_year, worknode_slot_shift_id
),

classes_with_multiple_volunteers as (
    select
        school_id,
        academic_year,
        count(distinct worknode_slot_shift_id) filter (where volunteer_count > 1) as classes_with_more_than_1_volunteer
    from slot_shift_volunteer_counts
    group by school_id, academic_year
),

-- One row per batch: whether ANY of its slot_shifts has ANY tagged volunteer at all.
batch_volunteer_flag as (
    select
        school_id,
        academic_year,
        sc_level_batch_id,
        bool_or(owner_user_id is not null) as batch_has_volunteer
    from batch_coverage
    group by school_id, academic_year, sc_level_batch_id
),

sections_without_volunteer as (
    select
        school_id,
        academic_year,
        count(distinct sc_level_batch_id) filter (where not batch_has_volunteer) as sections_without_volunteer
    from batch_volunteer_flag
    group by school_id, academic_year
),

active_students as (
    select student_id, is_active from {{ ref('stg_pc_student') }}
),

-- child_batch_enrollment already resolves school_id/academic_year per enrollment (batch_student)
-- row -- filtered here to the student's own active status, not the enrollment's, for the reason
-- given in the header comment above.
children_enrollment as (
    select
        cbe.school_id,
        cbe.academic_year,
        cbe.student_id,
        cbe.batch_id
    from {{ ref('int_pc_child_batch_enrollment') }} cbe
    join active_students s
        on cbe.student_id = s.student_id
        and s.is_active = true
),

children_in_system as (
    select
        school_id,
        academic_year,
        count(distinct student_id) as total_children_in_system
    from children_enrollment
    group by school_id, academic_year
),

children_mentor_split as (
    select
        ce.school_id,
        ce.academic_year,
        count(distinct ce.student_id) filter (
            where coalesce(bvf.batch_has_volunteer, false)
        ) as total_children_with_mentor,
        count(distinct ce.student_id) filter (
            where not coalesce(bvf.batch_has_volunteer, false)
        ) as children_without_mentor
    from children_enrollment ce
    left join batch_volunteer_flag bvf
        on ce.batch_id = bvf.sc_level_batch_id
        and ce.school_id = bvf.school_id
        and ce.academic_year = bvf.academic_year
    group by ce.school_id, ce.academic_year
),

-- The row set: a school/year must appear even if it has batches but zero enrolled children yet
-- (or vice versa), so this unions both populations rather than anchoring on just one -- same
-- precaution as E2's all_chapter_academic_years.
all_school_academic_years as (
    select school_id, academic_year from batch_coverage
    union
    select school_id, academic_year from children_enrollment
),

volunteers_in_school as (
    select
        school_id,
        count(distinct owner_user_id) as total_volunteers_in_school
    from batch_coverage
    group by school_id
)

select
    acay.school_id,
    acay.academic_year,
    sch.is_active as is_school_active,
    ba.total_sections,
    ba.total_slots,
    ba.total_classes,
    cis.total_children_in_system,
    cms.total_children_with_mentor,
    cms.children_without_mentor,
    svw.sections_without_volunteer,
    ba.total_volunteers_assigned,
    vis.total_volunteers_in_school,
    cmv.classes_with_more_than_1_volunteer,
    ba.classes_started,
    ba.classes_not_started
from all_school_academic_years acay
left join batch_agg ba
    on acay.school_id = ba.school_id
    and acay.academic_year = ba.academic_year
left join children_in_system cis
    on acay.school_id = cis.school_id
    and acay.academic_year = cis.academic_year
left join children_mentor_split cms
    on acay.school_id = cms.school_id
    and acay.academic_year = cms.academic_year
left join sections_without_volunteer svw
    on acay.school_id = svw.school_id
    and acay.academic_year = svw.academic_year
left join classes_with_multiple_volunteers cmv
    on acay.school_id = cmv.school_id
    and acay.academic_year = cmv.academic_year
left join volunteers_in_school vis
    on acay.school_id = vis.school_id
left join {{ ref('int_pc_school_id') }} sch
    on acay.school_id = sch.school_id
