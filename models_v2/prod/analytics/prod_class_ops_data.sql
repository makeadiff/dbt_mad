{{
  config(
    materialized='table',
    description='Class operations data: partner details, CO info, children count, volunteer assignments, slot/class metrics'
  )
}}

-- Optimized: Uses shared int_crm__active_partners instead of repeating 60+ lines of CTEs

WITH active_partners AS (
    SELECT * FROM {{ ref('int_crm__active_partners') }}
),

-- Children in Bubble (active children per school)
-- TODO: Integrate Platform Commons (PC) data for child level metrics
children_per_school AS (
    SELECT school_id, COUNT(*) AS active_child_count
    FROM {{ ref('dim_child') }}
    WHERE is_active = true
    GROUP BY school_id
),

-- Volunteers recruited per school
-- TODO: Integrate Platform Commons (PC) data for volunteer recruitment tracking
volunteers_per_school AS (
    SELECT school_id, COUNT(*) AS volunteer_count
    FROM {{ ref('fct_school_volunteer') }}
    WHERE is_removed = false
    GROUP BY school_id
),

-- Volunteers assigned to classes
volunteers_in_class AS (
    SELECT 
        cs.school_id,
        COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned_to_class
    FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs 
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('dim_class_section') }} cs 
        ON scs.class_section_id = cs.class_section_id
    WHERE scsv.is_removed = false 
      AND scs.is_removed = false
    GROUP BY cs.school_id
),

-- Current slot count per school (dim_slot already filters is_removed=false)
slot_counts AS (
    SELECT school_id, COUNT(DISTINCT slot_id) AS current_slot_count
    FROM {{ ref('dim_slot') }}
    WHERE school_id IS NOT NULL
    GROUP BY school_id
),

-- Reusable: class sections with children (used in 3 metrics below)
class_sections_with_children AS (
    SELECT cs.school_id,
        COUNT(DISTINCT ccs.class_section_id) AS total_count
    FROM {{ ref('bridge_child_class_section') }} ccs
    JOIN {{ ref('dim_class_section') }} cs
        ON ccs.class_section_id = cs.class_section_id
    WHERE ccs.is_removed = false
        AND cs.is_active = true
        AND cs.school_id IS NOT NULL
    GROUP BY cs.school_id
),

-- Current class count (scheduled classes with children)
current_classes AS (
    SELECT cs.school_id,
        COUNT(DISTINCT scs.slot_class_section_id) AS current_class_count
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    JOIN {{ ref('bridge_child_class_section') }} ccs ON ccs.class_section_id = cs.class_section_id AND ccs.is_removed = false
    WHERE scs.is_removed = false AND cs.is_active = true AND cs.school_id IS NOT NULL
    GROUP BY cs.school_id
),

-- Volunteer distribution across scheduled classes
class_volunteer_dist AS (
    SELECT
        cs.school_id,
        scs.slot_class_section_id,
        COUNT(DISTINCT scsv.volunteer_id) FILTER (WHERE scsv.is_removed = false) AS vol_count
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    JOIN {{ ref('bridge_child_class_section') }} ccs ON ccs.class_section_id = cs.class_section_id AND ccs.is_removed = false
    LEFT JOIN {{ ref('int_bubble__slot_class_section_volunteer') }} scsv ON scs.slot_class_section_id = scsv.slot_class_section_id
    WHERE scs.is_removed = false AND cs.is_active = true AND cs.school_id IS NOT NULL
    GROUP BY cs.school_id, scs.slot_class_section_id
),

-- Aggregate volunteer distribution counts
vol_dist_summary AS (
    SELECT
        school_id,
        COUNT(*) FILTER (WHERE vol_count = 1) AS classes_1_vol,
        COUNT(*) FILTER (WHERE vol_count = 2) AS classes_2_vol
    FROM class_volunteer_dist
    GROUP BY school_id
),

-- Unscheduled class sections (have children but no slot assignment)
unscheduled_counts AS (
    SELECT cs.school_id,
        COUNT(DISTINCT cs.class_section_id) AS unscheduled_count
    FROM {{ ref('dim_class_section') }} cs
    JOIN {{ ref('bridge_child_class_section') }} ccs ON ccs.class_section_id = cs.class_section_id AND ccs.is_removed = false
    LEFT JOIN {{ ref('int_bubble__slot_class_section') }} scs ON scs.class_section_id = cs.class_section_id AND scs.is_removed = false
    WHERE cs.is_active = true AND cs.school_id IS NOT NULL AND scs.slot_class_section_id IS NULL
    GROUP BY cs.school_id
),

-- Scheduled class sections (distinct class_section_ids that have slots)
scheduled_counts AS (
    SELECT cs.school_id,
        COUNT(DISTINCT scs.class_section_id) AS scheduled_count
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    JOIN {{ ref('bridge_child_class_section') }} ccs ON ccs.class_section_id = cs.class_section_id AND ccs.is_removed = false
    WHERE scs.is_removed = false AND cs.is_active = true AND cs.school_id IS NOT NULL
    GROUP BY cs.school_id
),

-- Average slot duration (dim_slot has pre-calculated duration_minutes)
avg_slot_duration AS (
    SELECT school_id, AVG(duration_minutes) AS avg_duration
    FROM {{ ref('dim_slot') }}
    WHERE school_id IS NOT NULL AND start_time IS NOT NULL AND end_time IS NOT NULL
    GROUP BY school_id
)

-- TODO: Integrate Platform Commons (PC) data for Partner/School Profile information
SELECT
    ap.partner_id                                               AS "Partner ID",
    ap.partner_name                                             AS "Partner Name",
    ap.co_id_numeric                                            AS "CO ID",
    ap.co_name                                                  AS "CO Name",
    COALESCE(ch.active_child_count, 0)                          AS "Children in Bubble",
    COALESCE(vps.volunteer_count, 0)                            AS "Volunteers Recruited",
    COALESCE(vic.volunteers_assigned_to_class, 0)               AS "Volunteers Assigned to Class",
    2                                                           AS "Ideal Slot Count",
    COALESCE(sc.current_slot_count, 0)                          AS "Current Slot Count",
    COALESCE(cswc.total_count, 0) * 2                           AS "Ideal Class Count",
    COALESCE(cc.current_class_count, 0)                         AS "Current Class Count",
    COALESCE(vds.classes_1_vol, 0)                              AS "Classes with 1 Volunteer",
    COALESCE(vds.classes_2_vol, 0)                              AS "Classes with 2 Volunteers",
    COALESCE(uc.unscheduled_count, 0)                           AS "Unscheduled Class Sections",
    COALESCE(uc.unscheduled_count, 0)                           AS "Classes with 0 Volunteers",
    COALESCE(scc.scheduled_count, 0)                            AS "Scheduled Class Sections",
    COALESCE(cswc.total_count, 0)                               AS "Class Sections with Children",
    CASE WHEN COALESCE(cswc.total_count, 0) > 0
        THEN ROUND((COALESCE(cc.current_class_count, 0) / (cswc.total_count * 2.0)) * 100, 2)
    END                                                         AS "Percentage Classes vs Ideal Classes",
    CASE WHEN COALESCE(cc.current_class_count, 0) > 0
        THEN ROUND(((COALESCE(vds.classes_1_vol, 0) + COALESCE(vds.classes_2_vol, 0)) / cc.current_class_count::numeric) * 100, 2)
    END                                                         AS "Percentage Classes with At Least 1 Volunteer",
    CASE WHEN COALESCE(cc.current_class_count, 0) > 0
        THEN ROUND((COALESCE(vds.classes_2_vol, 0) / cc.current_class_count::numeric) * 100, 2)
    END                                                         AS "Percentage Classes with 2 Volunteers",
    ROUND(COALESCE(asd.avg_duration, 0), 2)                     AS "Average Slot Duration (Minutes)"

FROM active_partners ap
LEFT JOIN children_per_school ch      ON ch.school_id = ap.partner_id
LEFT JOIN volunteers_per_school vps   ON vps.school_id = ap.partner_id
LEFT JOIN volunteers_in_class vic     ON vic.school_id = ap.partner_id
LEFT JOIN slot_counts sc              ON sc.school_id = ap.partner_id
LEFT JOIN class_sections_with_children cswc ON cswc.school_id = ap.partner_id
LEFT JOIN current_classes cc          ON cc.school_id = ap.partner_id
LEFT JOIN vol_dist_summary vds        ON vds.school_id = ap.partner_id
LEFT JOIN unscheduled_counts uc       ON uc.school_id = ap.partner_id
LEFT JOIN scheduled_counts scc        ON scc.school_id = ap.partner_id
LEFT JOIN avg_slot_duration asd       ON asd.school_id = ap.partner_id
ORDER BY ap.partner_name
