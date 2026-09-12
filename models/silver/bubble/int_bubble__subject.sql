{{ config(materialized='table') }}

-- Deduplicates subject records
-- Flow: stg_bubble__subject → int_bubble__subject
-- program_id comes pre-resolved from sessionops_raw (real bigint FK) - no more UUID join
-- needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        raw.subject_id,
        raw.subject_name,
        raw.is_removed,
        raw.program_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__subject') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='subject_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['subject_id']) }} as subject_sk,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }} as program_sk,
    subject_id,
    subject_name,
    is_removed,
    program_id,
    created_date,
    modified_date
from deduplicated
