{{ config(materialized='table') }}

-- Deduplicates school_academic_year records
-- Flow: stg_bubble__school_academic_year → int_bubble__school_academic_year
-- school_id and academic_year_id come pre-resolved from sessionops_raw (real bigint FKs) -
-- no more UUID join needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        raw.school_academic_year_id,
        raw.school_id,
        raw.academic_year_id,
        raw.is_active,
        raw.is_removed,
        raw.created_by_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_academic_year') }} raw
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
    school_academic_year_id,
    school_id,
    academic_year_id,
    is_active,
    is_removed,
    created_by_id as created_by,
    created_date,
    modified_date
from deduplicated
