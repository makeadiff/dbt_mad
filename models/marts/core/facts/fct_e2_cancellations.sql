{{ config(materialized='table') }}

-- fct_e2_cancellations: sessions cancelled due to school holidays, per slot_class_section per academic year
-- Grain: one row per (slot_class_section_id, academic_year)
-- Ported from the legacy fct_e2_cancellations model. Projects each section's weekly planned session
-- dates forward and checks them against that school's holiday calendar to count/label cancellations.
-- partner_id doubles as the school_id here, since fct_e2_volunteer_allocation_history.partner_id and
-- int_bubble__school_holiday.school_id both resolve into the same bubble partner_id1 domain.

with section_base as (
    select distinct on (va.slot_class_section_id, va.academic_year)
        va.partner_id,
        va.slot_class_section_id,
        va.academic_year,
        va.day_of_week,
        va.sessions_start_date,
        va.slot_class_sec_end_date
    from {{ ref('fct_e2_volunteer_allocation_history') }} va
    order by va.slot_class_section_id, va.academic_year, va.volunteer_id
),

allocation_with_dow as (
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
    from section_base
),

planned_sessions_calc as (
    select
        *,
        sessions_start_date::date as effective_start,
        least(
            current_date,
            coalesce(
                slot_class_sec_end_date::date,
                case
                    when academic_year = '2025-2026' then '2026-04-30'::date
                    when academic_year = '2026-2027' then '2027-04-30'::date
                    else null
                end
            )
        ) as effective_end
    from allocation_with_dow
),

section_with_first_session as (
    select
        *,
        case
            when slot_dow is not null and effective_start is not null
            then effective_start + ((slot_dow - extract(dow from effective_start)::int + 7) % 7)
            else null
        end as first_session_date
    from planned_sessions_calc
),

planned_dates as (
    select
        swfs.partner_id,
        swfs.slot_class_section_id,
        swfs.academic_year,
        swfs.first_session_date + (gs.n * 7) as planned_date
    from section_with_first_session swfs
    cross join generate_series(0, 52) as gs (n)
    where swfs.first_session_date is not null
        and swfs.effective_end is not null
        and swfs.first_session_date + (gs.n * 7) <= swfs.effective_end
),

cancelled_dates as (
    select distinct on (pd.slot_class_section_id, pd.planned_date)
        pd.slot_class_section_id,
        pd.academic_year,
        pd.planned_date,
        sh.holiday_reason
    from planned_dates pd
    join {{ ref('int_bubble__school_holiday') }} sh
        on pd.partner_id = sh.school_id
        and pd.planned_date >= sh.start_date
        and pd.planned_date <= sh.end_date
        and sh.is_removed = false
    order by pd.slot_class_section_id, pd.planned_date, sh.holiday_reason
),

cancellations_per_section as (
    select
        slot_class_section_id,
        academic_year,
        count(*) as total_cancellations,
        string_agg(distinct holiday_reason, '; ' order by holiday_reason) as cancellation_reasons
    from cancelled_dates
    group by slot_class_section_id, academic_year
)

select
    sb.partner_id,
    sb.slot_class_section_id,
    sb.academic_year,
    coalesce(cps.total_cancellations, 0) as total_cancellations,
    cps.cancellation_reasons
from section_base sb
left join cancellations_per_section cps
    on sb.slot_class_section_id = cps.slot_class_section_id
    and sb.academic_year = cps.academic_year
