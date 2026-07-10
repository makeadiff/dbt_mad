{{ config(
    materialized = 'table'
) }}

select *
from {{ ref('school_data_comparison') }}
