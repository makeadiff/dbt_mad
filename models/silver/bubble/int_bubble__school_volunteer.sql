{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_volunteer records + deduplicates
-- Flow: stg_bubble__school_volunteer → int_bubble__school_volunteer
-- Joins: partner (UUID→school_id), user (UUID→volunteer_id)

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
user_map as (
    select user_id as uuid, user_id_number as volunteer_id
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.school_volunteer_id,
        raw.academic_year,
        partner_map.school_id,
        user_map.volunteer_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_volunteer') }} raw
    left join partner_map on raw.school_id = partner_map.uuid
    left join user_map on raw.volunteer_id = user_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_volunteer_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_volunteer_id']) }} as school_volunteer_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    {{ dbt_utils.generate_surrogate_key(['volunteer_id']) }} as volunteer_sk,
    school_volunteer_id,
    academic_year,
    school_id,
    volunteer_id,
    is_removed,
    created_date,
    modified_date
from deduplicated
