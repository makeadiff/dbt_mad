{{ config(materialized='table') }}

-- Deduplicates school_session_detail records
-- Flow: stg_bubble__school_session_detail → int_bubble__school_session_detail
-- school_id and school_academic_year_id come pre-resolved from sessionops_raw (real bigint
-- FKs) - no more UUID joins needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        raw.session_id,
        raw.school_id,
        raw.school_academic_year_id,
        raw.start_date::date as start_date,
        raw.end_date::date as end_date,
        raw.is_active,
        raw.is_removed,
        raw.created_by_id as created_by,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_session_detail') }} raw
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
