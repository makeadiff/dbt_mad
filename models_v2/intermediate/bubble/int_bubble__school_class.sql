{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_class records + deduplicates
-- Flow: stg_bubble__school_class → int_bubble__school_class
-- Joins: class (UUID→class_id), partner (UUID→school_id)

with class_map as (
    select _id as uuid, class_id
    from {{ ref('stg_bubble__class') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),

joined as (
    select
        raw.school_class_id,
        class_map.class_id,
        partner_map.school_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_class') }} raw
    left join class_map on raw.class_id = class_map.uuid
    left join partner_map on raw.school_id = partner_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_class_id',
        order_by='modified_date desc',
       )
    }}
)

select * from deduplicated
