{{ config(materialized='table') }}

SELECT * FROM {{ ref('prod_user_school_mapping') }}
