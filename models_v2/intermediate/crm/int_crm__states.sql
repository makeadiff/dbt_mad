{{ config(materialized='table') }}

-- Deduplicated state master data

with source_data as (
    select * from {{ ref('stg_crm__states') }}
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by='state_id',
        order_by='_airbyte_extracted_at desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['state_id']) }} as state_sk,
    state_id,
    state_name,
    _airbyte_extracted_at
from deduplicated
