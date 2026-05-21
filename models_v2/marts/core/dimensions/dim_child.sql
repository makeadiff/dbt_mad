{{ config(materialized='table') }}

-- dim_child: One row per child
-- Source: int_bubble__child (resolves school_id via UUID joins)

select
    child_sk,
    child_id,
    first_name,
    last_name,
    gender,
    dob,
    age,
    city,
    date_of_enrollment,
    mother_tongue,
    is_active,
    is_removed,
    class_id,
    school_class_id,
    school_id,
    created_date,
    modified_date
from {{ ref('int_bubble__children') }}
where is_removed = false
