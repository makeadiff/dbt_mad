{{ config(materialized='table') }}

select
    id::bigint as final_amount_id,
    "currency"::text as currency,
    "value"::numeric(10,2) as amount_value
from {{ source('pc_raw', 'finalAmount') }}
where "xIsDeleted" is false or "xIsDeleted" is null
