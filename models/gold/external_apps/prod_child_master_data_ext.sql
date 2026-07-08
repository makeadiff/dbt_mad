{{ config(materialized='table') }}

-- Sourced from int_bubble__children (not dim_child) so removed children are still
-- included here, matching the old child_int model's behavior.
SELECT
    child_id,
    first_name,
    last_name,
    gender,
    dob,
    city,
    date_of_enrollment,
    mother_tongue AS mother_tounge,
    age,
    is_active,
    is_removed AS removed,
    class_id,
    school_class_id,
    school_id,
    created_date,
    modified_date
FROM {{ ref('int_bubble__children') }}
