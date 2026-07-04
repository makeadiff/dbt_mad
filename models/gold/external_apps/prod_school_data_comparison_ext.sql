{{ config(materialized='table') }}

SELECT * FROM {{ ref('prod_school_data_comparison') }}
