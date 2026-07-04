{{ config(materialized='table') }}

-- User School Chapter Mapping: CO-to-converted-partner mapping
-- Sourced from Platform Commons user data for CO assignments

WITH active_partners AS (
    SELECT 
        partner_id AS school_id, 
        partner_name AS school_name, 
        co_user_id, 
        co_name,
        co_email
    FROM {{ ref('int_crm__active_partners') }}
)

SELECT 
  school_id, 
  school_name,
  co_user_id AS user_id,
  co_name AS user_name,
  co_email AS user_email
FROM active_partners
ORDER BY school_name, co_name
