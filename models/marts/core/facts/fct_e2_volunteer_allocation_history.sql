{{ config(materialized='table') }}

-- fct_e2_volunteer_allocation_history: volunteer-to-slot assignment history with computed start/end dates
-- Grain: one row per (volunteer_id, slot_class_section_id) assignment period
-- Ported from the legacy volunteer_allocation_history_e2_sessions model. Lives entirely in
-- marts/core so it does not depend on gold/external_apps — prod_volunteer_allocation_history
-- should reference this model rather than own the allocation logic itself.

with scsv_deduped as (
    -- Airbyte CDC can write duplicate removed=TRUE rows for the same volunteer+section.
    -- Deduplicate on (slot_class_section_id, volunteer_id, is_removed, created_date::date) so that
    -- legitimate reassignments (one removed + one active row) are preserved as two rows.
    select distinct on (slot_class_section_id, volunteer_id, is_removed, created_date::date)
        *
    from {{ ref('int_bubble__slot_class_section_volunteer') }}
    order by slot_class_section_id, volunteer_id, is_removed, created_date::date, modified_date desc
),

scsv_with_next as (
    -- modified_date is unreliable as a removal timestamp because schema changes and bulk archive
    -- operations update it on already-removed rows. When a volunteer was re-assigned to the same
    -- slot_class_section, the next assignment's created_date is a tighter upper bound on when the
    -- previous assignment actually ended. least() takes whichever is earlier.
    select
        *,
        lead(created_date) over (
            partition by slot_class_section_id, volunteer_id
            order by created_date
        ) as next_assignment_start
    from scsv_deduped
)

select
    scsv.volunteer_id,
    scsv.slot_class_section_id,
    scsv.created_date as slot_class_sec_start_date,
    case
        when scs.is_removed then scs.modified_date
        when scsv.is_removed or scsv.is_active = false then
            case
                when scsv.next_assignment_start is not null
                    then least(scsv.next_assignment_start, scsv.modified_date)
                else scsv.modified_date
            end
        else null
    end as slot_class_sec_end_date,
    (not scs.is_removed and not scsv.is_removed and coalesce(scsv.is_active, true)) as is_active,
    scs.slot_id,
    scs.class_section_subject_id,
    cs.section_name,
    p.partner_id,
    p.partner_name,
    ud.user_display_name,
    s.day_of_week,
    s.slot_name,
    ay.label as academic_year,
    coalesce(
        ssd.start_date,
        case say.academic_year_id
            when 1 then '2025-07-15'::date
            when 2 then '2026-07-15'::date
            else null
        end
    ) as sessions_start_date
from scsv_with_next scsv
join {{ ref('int_bubble__slot_class_section') }} scs
    on scsv.slot_class_section_id = scs.slot_class_section_id
join {{ ref('int_bubble__class_section') }} cs
    on scs.class_section_id = cs.class_section_id
left join {{ ref('int_bubble__user') }} ud
    on scsv.volunteer_id = ud.volunteer_id
join {{ ref('int_bubble__slot') }} s
    on scs.slot_id = s.slot_id
left join {{ ref('int_bubble__school_academic_year') }} say
    on s.school_academic_year_id = say.school_academic_year_id
left join {{ ref('int_bubble__academic_year') }} ay
    on say.academic_year_id = ay.academic_year_id
left join {{ ref('int_bubble__school_session_detail') }} ssd
    on say.school_academic_year_id = ssd.school_academic_year_id
    and cs.school_id = ssd.school_id
left join {{ ref('int_bubble__partner') }} p
    on cs.school_id = p.partner_id
where p.partner_id is not null
