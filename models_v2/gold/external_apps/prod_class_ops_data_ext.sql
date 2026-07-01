{{ config(materialized='table') }}

SELECT * FROM {{ ref('prod_class_ops_data') }}
