{{ config(
    materialized = 'table'
) }}

select *
from {{ ref('class_ops_data') }}
