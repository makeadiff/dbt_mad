{{ config(materialized='table') }}

-- Deduplicates academic_year records (Airbyte CDC can emit multiple rows per academic_year_id)
-- Flow: stg_bubble__academic_year → int_bubble__academic_year

with source_data as (
    select * from {{ ref('stg_bubble__academic_year') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='academic_year_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['academic_year_id']) }} as academic_year_sk,
    academic_year_id,
    label,
    is_active,
    created_date,
    modified_date
from deduplicated
