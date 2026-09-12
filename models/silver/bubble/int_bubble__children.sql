{{ config(materialized='table') }}

-- Deduplicates child records + resolves a child's current class/school_class assignment.
-- Flow: stg_bubble__children → int_bubble__child
-- school_id comes pre-resolved from sessionops_raw (real bigint FK) - no more UUID join
-- needed for it now that bronze sources from sessionops_raw.
-- class_id/school_class_id no longer live on child itself in sessionops (that link now lives
-- only in the child_class join table) - resolved here via int_bubble__child_class (->
-- int_bubble__school_class for class_id) so downstream consumers (dim_child,
-- prod_child_master_data_ext) keep the same "current class" columns they had before. Picks the
-- child's most recently created non-removed child_class row when more than one exists.

with current_class as (
    select distinct on (cc.child_id)
        cc.child_id,
        cc.school_class_id,
        sc.class_id
    from {{ ref('int_bubble__child_class') }} cc
    left join {{ ref('int_bubble__school_class') }} sc
        on cc.school_class_id = sc.school_class_id
    where cc.is_removed = false
    order by cc.child_id, cc.created_date desc
),

joined as (
    select
        raw.child_id,
        raw.first_name,
        raw.last_name,
        raw.gender,
        raw.dob,
        raw.city,
        raw.date_of_enrollment,
        raw.mother_tongue,
        raw.age,
        raw.is_active,
        raw.is_removed,
        current_class.class_id,
        current_class.school_class_id,
        raw.school_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__children') }} raw
    left join current_class on raw.child_id = current_class.child_id
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_id']) }} as child_sk,
    child_id,
    first_name,
    last_name,
    gender,
    dob,
    city,
    date_of_enrollment,
    mother_tongue,
    age,
    is_active,
    is_removed,
    class_id,
    school_class_id,
    school_id,
    created_date,
    modified_date
from deduplicated
