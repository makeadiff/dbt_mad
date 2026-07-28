{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_session_detail records + deduplicates
-- Flow: stg_bubble__school_session_detail → int_bubble__school_session_detail
-- Joins: partner (UUID→school_id), school_academic_year (UUID→school_academic_year_id)

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
school_academic_year_map as (
    select school_academic_year_uuid as uuid, school_academic_year_id
    from {{ ref('int_bubble__school_academic_year') }}
),

joined as (
    select
        raw.session_id,
        partner_map.school_id,
        school_academic_year_map.school_academic_year_id,
        raw.start_date::date as start_date,
        raw.end_date::date as end_date,
        raw.is_active,
        raw.is_removed,
        raw.created_by,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_session_detail') }} raw
    left join partner_map on raw.school_id = partner_map.uuid
    left join school_academic_year_map on raw.school_academic_year_id = school_academic_year_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='session_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['session_id']) }} as school_session_detail_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    {{ dbt_utils.generate_surrogate_key(['school_academic_year_id']) }} as school_academic_year_sk,
    session_id,
    school_id,
    school_academic_year_id,
    start_date,
    end_date,
    is_active,
    is_removed,
    created_by,
    created_date,
    modified_date
from deduplicated
