{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_academic_year records + deduplicates
-- Flow: stg_bubble__school_academic_year → int_bubble__school_academic_year
-- Joins: partner (UUID→school_id), academic_year (UUID→academic_year_id)
-- Exposes school_academic_year_uuid so other bubble entities (school_class, slot,
-- batch_child, school_session_detail) can resolve their school_academic_year_id UUID
-- FK into this table's integer school_academic_year_id.

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
academic_year_map as (
    select "_id" as uuid, academic_year_id
    from {{ ref('stg_bubble__academic_year') }}
),

joined as (
    select
        raw."_id" as school_academic_year_uuid,
        raw.school_academic_year_id,
        partner_map.school_id,
        academic_year_map.academic_year_id,
        raw.is_active,
        raw.is_removed,
        raw.created_by,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_academic_year') }} raw
    left join partner_map on raw.school_id = partner_map.uuid
    left join academic_year_map on raw.academic_year_id = academic_year_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_academic_year_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_academic_year_id']) }} as school_academic_year_sk,
    school_academic_year_uuid,
    school_academic_year_id,
    school_id,
    academic_year_id,
    is_active,
    is_removed,
    created_by,
    created_date,
    modified_date
from deduplicated
