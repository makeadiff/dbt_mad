{{ config(materialized='table', tags=["user_data"]) }}

SELECT
    *
FROM {{ ref('int_pc_user_data') }}
