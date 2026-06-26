{{ config(materialized='table') }}

-- School-level volunteer metrics (shared across prod_class_ops_data, prod_volunteer_recruitment)
-- Eliminates duplicate volunteers_per_school + volunteers_in_class CTEs

WITH recruited AS (
    SELECT school_id, COUNT(*) AS volunteers_recruited
    FROM {{ ref('fct_school_volunteer') }}
    WHERE is_removed = false
    GROUP BY school_id
),

assigned AS (
    SELECT cs.school_id, COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned_to_class
    FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON scs.class_section_id = cs.class_section_id
    WHERE scsv.is_removed = false AND scs.is_removed = false
    GROUP BY cs.school_id
)

SELECT
    COALESCE(r.school_id, a.school_id) AS school_id,
    COALESCE(r.volunteers_recruited, 0) AS volunteers_recruited,
    COALESCE(a.volunteers_assigned_to_class, 0) AS volunteers_assigned_to_class
FROM recruited r
FULL OUTER JOIN assigned a ON r.school_id = a.school_id
