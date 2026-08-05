{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_class records + deduplicates
-- Flow: stg_bubble__school_class → int_bubble__school_class
-- Joins: class (UUID→class_id), partner (UUID→school_id), school_academic_year (UUID→school_academic_year_id)
-- school_academic_year_id lets downstream models (e.g. fct_e2_school_coverage) tie a school's classes
-- to a specific academic_year_int label.

with class_map as (
    select _id as uuid, class_id
    from {{ ref('stg_bubble__class') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
school_academic_year_map as (
    select school_academic_year_uuid as uuid, school_academic_year_id
    from {{ ref('int_bubble__school_academic_year') }}
),

joined as (
    select
        raw.school_class_id,
        class_map.class_id,
        partner_map.school_id,
        school_academic_year_map.school_academic_year_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_class') }} raw
    left join class_map on raw.class_id = class_map.uuid
    left join partner_map on raw.school_id = partner_map.uuid
    left join school_academic_year_map on raw.school_academic_year_id = school_academic_year_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_class_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    {{ dbt_utils.generate_surrogate_key(['class_id']) }} as class_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    school_class_id,
    class_id,
    school_id,
    school_academic_year_id,
    is_removed,
    created_date,
    modified_date
from deduplicated
