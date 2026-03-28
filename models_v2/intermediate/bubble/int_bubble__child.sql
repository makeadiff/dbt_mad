{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child records + deduplicates
-- Flow: stg_bubble__children → int_bubble__child
-- Joins: class (UUID→class_id), school_class (UUID→school_class_id), partner (UUID→school_id)

with class_map as (
    select _id as uuid, class_id
    from {{ ref('stg_bubble__class') }}
),
school_class_map as (
    select _id as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
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
        class_map.class_id,
        school_class_map.school_class_id,
        partner_map.school_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__children') }} raw
    left join class_map on raw.class_id = class_map.uuid
    left join school_class_map on raw.school_class_id = school_class_map.uuid
    left join partner_map on raw.school_id = partner_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_id',
        order_by='modified_date desc',
       )
    }}
)

select * from deduplicated
