{{ config(materialized='table') }}

WITH cleaned AS (
    SELECT
        user_id,
        city,
        email,
        state,
        center,
        added_by,
        contact,
        user_role,
        user_login,
        user_display_name,
        reporting_manager_user_id,
        reporting_manager_role_code,
        reporting_manager_user_login,
        user_created_datetime,
        user_updated_datetime,
        RIGHT(REGEXP_REPLACE(contact, '\D', '', 'g'), 10) AS norm_10
    FROM {{ ref('user_data_int') }}
    WHERE contact IS NOT NULL 
      AND contact != ''
),

dupe_norms AS (
    SELECT norm_10
    FROM cleaned
    WHERE LENGTH(norm_10) = 10
    GROUP BY norm_10
    HAVING COUNT(*) > 1
)

SELECT 
    c.city,
    c.email,
    c.state,
    c.center,
    c.user_id,
    c.added_by,
    c.contact,
    c.user_role,
    c.user_login,
    c.user_display_name,
    c.reporting_manager_user_id,
    c.reporting_manager_role_code,
    c.reporting_manager_user_login,
    c.user_created_datetime,
    c.user_updated_datetime,
    c.norm_10,
    COUNT(*) OVER (PARTITION BY c.norm_10) as contact_count
FROM cleaned c
JOIN dupe_norms d USING (norm_10)
ORDER BY c.norm_10, c.user_id
