{{ config(materialized='table') }}

select
    id::bigint as tip_id,
    "currency"::text as currency,
    "value"::numeric(10,2) as tip_value
from {{ source('pc_raw', 'tip') }}
where "xIsDeleted" is false or "xIsDeleted" is null
