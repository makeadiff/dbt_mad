-- fct_volunteer_slot_assignment: One row per volunteer assigned to one slot-class-section
-- Grain: One record per volunteer assigned to a specific teaching slot-section combination

select
    scsv.volunteer_assignment_sk,
    scsv.volunteer_sk,
    scs.slot_sk,
    scs.class_section_sk,
    scs.slot_class_section_sk,
    scsv.slot_class_section_volunteer_id,
    scsv.volunteer_id,
    scs.slot_id,
    scs.class_section_id,
    scs.class_section_subject_id,
    scsv.academic_year,
    scsv.created_date as assigned_date,
    scsv.modified_date,
    case 
        when scsv.is_removed = false then (current_date - scsv.created_date::date)
        else (scsv.modified_date::date - scsv.created_date::date)
    end as days_since_assignment,
    case when scsv.is_removed = true then false else true end as is_active,
    scsv.is_removed,
    scs.is_active as slot_is_active
from {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
left join {{ ref('int_bubble__slot_class_section') }} scs
    on scsv.slot_class_section_id = scs.slot_class_section_id
where scsv.is_removed = false
