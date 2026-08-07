{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_class records + deduplicates
-- Flow: stg_bubble__child_class → int_bubble__child_class
-- Joins: child (UUID→child_id), school_class (UUID→school_class_id)
-- This is the more fundamental child enrollment link: a child must be assigned to a school_class
-- (mandatory at enrollment/edit time) even before, or without ever, being assigned a specific
-- class_section -- so this reaches children that int_bubble__child_class_section misses.

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
school_class_map as (
    select _id as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
),

joined as (
    select
        raw.child_class_id,
        raw.academic_year,
        child_map.child_id,
        school_class_map.school_class_id,
        raw.is_active,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_class') }} raw
    left join child_map on raw.child_id = child_map.uuid
    left join school_class_map on raw.school_class_id = school_class_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_class_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_class_id']) }} as child_class_sk,
    {{ dbt_utils.generate_surrogate_key(['child_id']) }} as child_sk,
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    child_class_id,
    academic_year,
    child_id,
    school_class_id,
    is_active,
    is_removed,
    created_date,
    modified_date
from deduplicated
