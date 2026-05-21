{{ config(materialized='table') }}

-- Resolves UUID foreign keys for class_section records + deduplicates
-- Flow: stg_bubble__class_section → int_bubble__class_section
-- Joins: school_class (UUID→school_class_id), partner (UUID→school_id)

with school_class_map as (
    select _id as uuid, school_class_id
    from {{ ref('stg_bubble__school_class') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),

joined as (
    select
        raw.class_section_id,
        raw.academic_year,
        raw.section_name,
        raw.is_removed,
        raw.is_active,
        school_class_map.school_class_id,
        partner_map.school_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__class_section') }} raw
    left join school_class_map on raw.school_class_id = school_class_map.uuid
    left join partner_map on raw.school_id = partner_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='class_section_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['class_section_id']) }} as class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    class_section_id,
    academic_year,
    section_name,
    is_removed,
    is_active,
    school_class_id,
    school_id,
    created_date,
    modified_date
from deduplicated
