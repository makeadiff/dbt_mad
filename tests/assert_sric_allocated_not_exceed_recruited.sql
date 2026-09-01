-- volunteers_unallocated = GREATEST(0, recruited - allocated) floors at 0 rather than going
-- negative, matching the GREATEST(0, ...) pattern already used for the recruitment/sourcing
-- targets in this model. Unlike recruitment_target_met_pct (§4, documented exception -- an
-- achievement-vs-target ratio that can legitimately exceed 100%), "allocated" and "recruited"
-- here come from two independently-computed populations that are supposed to nest (allocated
-- should be a subset of recruited) -- allocated > recruited is a real inconsistency the clamp
-- would otherwise hide. This monitors for that directly rather than trusting the clamped output.
select
    esc.chapter_id,
    svm.volunteers_recruited,
    alloc.volunteers_currently_allocated
from (
    select distinct m.chapter_id, p.bubble_partner_id as school_id
    from {{ ref('int_google_sheet__chapter_mapping') }} m
    inner join {{ ref('dim_bubble_partner') }} p on m.chapter_id::integer = p.bubble_partner_id::integer
    where m.engine = 'E2'
) esc
inner join {{ ref('int_bubble__school_volunteer_metrics') }} svm
    on esc.school_id = svm.school_id
inner join (
    select cs.school_id, count(distinct scsv.volunteer_id) as volunteers_currently_allocated
    from {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    inner join {{ ref('int_bubble__slot_class_section') }} scs
        on scsv.slot_class_section_id = scs.slot_class_section_id
    inner join {{ ref('dim_class_section') }} cs
        on scs.class_section_id = cs.class_section_id
    where scsv.is_removed = false and scs.is_removed = false and scsv.is_active = true
    group by cs.school_id
) alloc
    on esc.school_id = alloc.school_id
where alloc.volunteers_currently_allocated > svm.volunteers_recruited
