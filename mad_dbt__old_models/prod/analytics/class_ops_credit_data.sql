{{ config(materialized='view') }}

WITH filtered_class_ops AS (
    SELECT
        volunteer_id,
        academic_year
    FROM {{ ref('class_ops_master_data_int') }}
    WHERE academic_year = 'ACADEMIC_SESSION.2025-2026'
        AND batch_status = 'ACTIVE'
        AND volunteer_id IS NOT NULL
),

unique_volunteers AS (
    SELECT DISTINCT
        volunteer_id,
        academic_year
    FROM filtered_class_ops
)

SELECT
    uv.volunteer_id,
    uv.academic_year,
    cd.*
FROM unique_volunteers uv
LEFT JOIN {{ ref('credit_data_int') }} cd
    ON uv.volunteer_id::integer = cd.user_id::integer

