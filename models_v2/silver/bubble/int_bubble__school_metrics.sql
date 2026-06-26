{{ config(materialized='table') }}

-- School-level child metrics (shared across prod_class_ops_data, prod_volunteer_recruitment, prod_school_data_comparison)
-- Eliminates duplicate children_per_school / active_children / dropped_children CTEs

SELECT
    school_id,
    COUNT(*) FILTER (WHERE is_active = true)  AS active_child_count,
    COUNT(*) FILTER (WHERE is_active = false) AS dropped_child_count
FROM {{ ref('dim_child') }}
GROUP BY school_id
