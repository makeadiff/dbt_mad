{{ config(materialized='table') }}

-- dim_school_academic_year_status: whether a school's academic-year mapping is currently active
-- Grain: one row per (school_id, academic_year)
-- A school can have more than one school_academic_year_id row mapped to the same
-- (school_id, academic_year_id) business key over time (e.g. recreated after being removed once,
-- or reactivated) -- int_bubble__school_academic_year only dedupes down to one row per PK, not per
-- business key, so this picks the most recently modified PK per (school_id, academic_year_id) as
-- authoritative and reads its is_active/is_removed flags as that school's status for that year.
-- Deliberately not folded into int_bubble__school_academic_year itself: that model is a shared FK
-- resolution table (school_class, slot, school_session_detail all join to it by school_academic_year_id
-- PK) and collapsing it to one row per business key would silently drop the superseded PKs those
-- joins rely on.

with latest_per_school_ay as (
    select distinct on (say.school_id, say.academic_year_id)
        say.school_id,
        say.academic_year_id,
        say.is_active,
        say.is_removed
    from {{ ref('int_bubble__school_academic_year') }} say
    order by say.school_id, say.academic_year_id, say.modified_date desc, say.created_date desc
)

select
    l.school_id,
    ay.label as academic_year,
    (l.is_active = true and l.is_removed = false) as is_ay_active
from latest_per_school_ay l
join {{ ref('int_bubble__academic_year') }} ay
    on l.academic_year_id = ay.academic_year_id
