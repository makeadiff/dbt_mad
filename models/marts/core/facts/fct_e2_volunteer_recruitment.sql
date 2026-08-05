{{ config(materialized='table') }}

-- fct_e2_volunteer_recruitment: total volunteers currently in the school_volunteer bucket, per chapter
-- Grain: one row per (chapter_id, academic_year) -- currently only ever '2026-2027'
-- school_volunteer was the 2025-2026 recruitment intake bucket (COs added volunteers here, then
-- manually assigned them into slots); that workflow was retired going into 2026-2027, but the table
-- itself still reflects the live/current set of volunteers considered "in" a school. 2025-2026 is
-- deliberately left out for now -- no reliable filter for "recruited last year" has been confirmed
-- yet, so that branch is parked rather than guessed at.

select
    sv.school_id::text as chapter_id,
    '2026-2027' as academic_year,
    count(distinct sv.volunteer_id) as total_volunteers_in_school
from {{ ref('int_bubble__school_volunteer') }} sv
where sv.is_active = true and sv.is_removed = false
group by sv.school_id
