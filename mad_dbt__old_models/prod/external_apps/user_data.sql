{{ config(materialized='table', tags=["user_data"]) }}

SELECT
   * 
FROM {{ ref('user_data_int') }} 