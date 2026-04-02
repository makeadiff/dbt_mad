-- fct_school_volunteer: One row per volunteer assigned to one school
-- Grain: One record per volunteer assigned to one school in one academic year

select
    school_volunteer_sk,
    school_sk,
    volunteer_sk,
    school_volunteer_id,
    school_id,
    volunteer_id,
    academic_year,
    created_date as assigned_date,
    modified_date,
    case
        when is_removed = false
        then (current_date - created_date::date)
        else (modified_date::date - created_date::date)
    end as days_assigned,
    not is_removed as is_active,
    is_removed
from {{ ref('int_bubble__school_volunteer') }}
where is_removed = false
