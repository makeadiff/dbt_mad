{{ config(materialized='table') }}

-- School Volunteer Children Summary: Per-school metrics for active partner schools
-- Optimized: Uses int_crm__active_partners + shared int models

WITH base AS (
    SELECT partner_id AS school_id, partner_name AS school_name
    FROM {{ ref('int_crm__active_partners') }}
),

volunteer_classes AS (
    SELECT
        b.school_id,
        b.school_name,
        COUNT(CASE WHEN scsv.volunteer_id IS NOT NULL THEN 1 END) AS classes_with_volunteers,
        COUNT(CASE WHEN scsv.volunteer_id IS NULL THEN 1 END)     AS classes_without_volunteers
    FROM base b
    LEFT JOIN {{ ref('dim_slot') }} sl ON b.school_id = sl.school_id
    LEFT JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON sl.slot_id = scs.slot_id AND scs.is_removed = false
    LEFT JOIN {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
        ON scs.slot_class_section_id = scsv.slot_class_section_id AND scsv.is_removed = false
    GROUP BY b.school_id, b.school_name
)

-- TODO: Integrate Platform Commons (PC) data for confirmed child counts and active child tracking
SELECT
    vc.school_id,
    vc.school_name,
    vc.classes_with_volunteers,
    vc.classes_without_volunteers,
    COALESCE(mou.confirmed_child_count, 0) AS confirmed_children,
    COALESCE(sm.active_child_count, 0)     AS children_in_system
FROM volunteer_classes vc
LEFT JOIN {{ ref('int_bubble__school_metrics') }} sm ON vc.school_id = sm.school_id
LEFT JOIN LATERAL (
    SELECT confirmed_child_count FROM {{ ref('dim_mou') }}
    WHERE partner_id = vc.school_id AND mou_status = 'active'
    ORDER BY created_at DESC LIMIT 1
) mou ON TRUE
ORDER BY vc.school_name
