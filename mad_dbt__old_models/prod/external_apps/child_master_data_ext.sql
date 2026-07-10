{{ config(
    materialized = 'table'
) }}

select *
from {{ ref('child_int') }}
