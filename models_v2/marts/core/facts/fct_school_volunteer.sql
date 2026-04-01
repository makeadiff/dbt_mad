{{ config(materialized='table') }}

-- fct_school_volunteer: One row per volunteer assigned to one school
-- Source: int_bubble__school_volunteer

select
    {{ dbt_utils.generate_surrogate_key(['school_volunteer_id']) }} as school_volunteer_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    {{ dbt_utils.generate_surrogate_key(['volunteer_id']) }} as volunteer_sk,
    school_volunteer_id,
    school_id,
    volunteer_id,
    academic_year,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__school_volunteer') }}
where is_removed = false
