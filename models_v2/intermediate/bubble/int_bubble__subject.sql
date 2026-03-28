{{ config(materialized='table') }}

-- Resolves UUID foreign keys for subject records + deduplicates
-- Flow: stg_bubble__subject → int_bubble__subject
-- Joins: program (UUID→program_id)

with program_map as (
    select _id as uuid, program_id
    from {{ ref('stg_bubble__program') }}
),

joined as (
    select
        raw.subject_id,
        raw.subject_name,
        raw.is_removed,
        program_map.program_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__subject') }} raw
    left join program_map on raw.program_id = program_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='subject_id',
        order_by='modified_date desc',
       )
    }}
)

select * from deduplicated
