{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'reasons') }}
)

select
    generated_id::bigint as reason_id,
    "value"::text as reason_value
from raw
