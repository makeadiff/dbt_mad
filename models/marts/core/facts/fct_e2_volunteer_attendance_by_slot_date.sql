{{ config(materialized='table') }}

-- fct_e2_volunteer_attendance_by_slot_date: volunteer attendance snapped to the slot's actual calendar date
-- Grain: one row per (volunteer_id, slot_class_section_id, date_of_slot) attendance submission
-- Ported from the legacy bot_attendance_actual_slot_date model. DOTS submissions are typically logged
-- a few days after the actual session, so this snaps entry_last_updated_date back to the most recent
-- occurrence of the slot's day_of_week, letting attendance roll up to the correct session date.

with source as (
    select
        partner_id,
        partner_name,
        volunteer_id,
        volunteers as volunteer_name,
        slot_class_section_id,
        attendance,
        day_of_week,
        entry_last_updated_date,
        substitute_volunteer_mad_id
    from {{ ref('int_dots__volunteer_attendance') }}
),

with_submission_date as (
    select
        *,
        entry_last_updated_date::date as submission_date
    from source
),

with_slot_dow as (
    select
        *,
        case trim(day_of_week)
            when 'Sunday' then 0
            when 'Monday' then 1
            when 'Tuesday' then 2
            when 'Wednesday' then 3
            when 'Thursday' then 4
            when 'Friday' then 5
            when 'Saturday' then 6
            else null
        end as slot_dow
    from with_submission_date
),

with_snapped_date as (
    select
        *,
        case
            when slot_dow is not null and submission_date is not null
            then submission_date - ((extract(dow from submission_date)::int - slot_dow + 7) % 7)
            else null
        end as snapped_date
    from with_slot_dow
)

select
    wsd.partner_id as chapter_id,
    wsd.partner_name as chapter_name,
    wsd.volunteer_id,
    wsd.volunteer_name,
    wsd.slot_class_section_id,
    wsd.attendance,
    case
        when nullif(trim(wsd.substitute_volunteer_mad_id), '') ~ '^[0-9]+$'
        then nullif(trim(wsd.substitute_volunteer_mad_id), '')::integer
        else null
    end as substitute_volunteer_id,
    (
        upper(trim(wsd.attendance)) = 'FALSE'
        and nullif(trim(wsd.substitute_volunteer_mad_id), '') ~ '^[0-9]+$'
    ) as is_substitute,
    wsd.day_of_week,
    wsd.entry_last_updated_date,
    wsd.snapped_date::timestamp as date_of_slot,
    ay.label as academic_year
from with_snapped_date wsd
left join {{ ref('int_bubble__slot_class_section') }} scs
    on wsd.slot_class_section_id = scs.slot_class_section_id
left join {{ ref('int_bubble__slot') }} s
    on scs.slot_id = s.slot_id
left join {{ ref('int_bubble__school_academic_year') }} say
    on s.school_academic_year_id = say.school_academic_year_id
left join {{ ref('int_bubble__academic_year') }} ay
    on say.academic_year_id = ay.academic_year_id
