{{ config(materialized='table') }}

-- dim_chapter_current_status: whether a chapter is active as of its single most recent academic-year record
-- Grain: one row per chapter (school_id)
-- "Dropped out" isn't something a single year's is_active/is_removed flags can prove -- a school whose
-- 2025-2026 record is inactive is often just showing normal year-end rollover (confirmed: of 101 schools
-- inactive for 2025-2026, 62 are active again in 2026-2027). This looks at only the chapter's latest
-- known academic-year record and reports whether THAT one is active, making no claim about earlier years.
-- Distinct from dim_school_academic_year_status, which stays per-year -- that one is still needed for the
-- slot/class/volunteer active-vs-archived counting logic in fct_e2_school_coverage, which genuinely
-- needs a per-year answer. This dimension is chapter-level and generalizes to however many academic
-- years exist -- it always picks whichever is latest by academic_year_id, not a hardcoded pair.

with ranked as (
    select
        sas.school_id,
        sas.partner_name,
        sas.academic_year,
        sas.is_ay_active,
        row_number() over (partition by sas.school_id order by ay.academic_year_id desc) as rn
    from {{ ref('dim_school_academic_year_status') }} sas
    join {{ ref('int_bubble__academic_year') }} ay
        on sas.academic_year = ay.label
)

select
    school_id,
    partner_name,
    academic_year as latest_academic_year,
    is_ay_active as is_currently_active
from ranked
where rn = 1
