{{ config(materialized='table') }}

-- Deduplicates class records (Airbyte CDC can emit multiple rows per class_id)
-- Flow: stg_bubble__class → int_bubble__class

with source_data as (
    select * from {{ ref('stg_bubble__class') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='class_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['class_id']) }} as class_sk,
    class_id,
    class_name,
    class_code,
    program_id,
    created_date,
    modified_date
from deduplicated
