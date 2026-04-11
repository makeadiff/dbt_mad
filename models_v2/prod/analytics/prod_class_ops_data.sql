{{
  config(
    materialized='table',
    description='Class operations data: partner details, CO info, children count, volunteer assignments, slot/class metrics'
  )
}}

-- Optimized: Uses shared int models for child/volunteer counts + collapsed class scheduling CTEs

WITH active_partners AS (
    SELECT * FROM {{ ref('int_crm__active_partners') }}
),

-- Slot count per school (dim_slot already filters is_removed=false)
slot_counts AS (
    SELECT school_id, COUNT(DISTINCT slot_id) AS current_slot_count
    FROM {{ ref('dim_slot') }}
    WHERE school_id IS NOT NULL
    GROUP BY school_id
),

-- Collapsed: class_sections_with_children + current_classes + scheduled + unscheduled in one pass
class_scheduling AS (
    SELECT
        cs.school_id,
        COUNT(DISTINCT cs.class_section_id)            AS class_sections_with_children,
        COUNT(DISTINCT scs.slot_class_section_id)       AS current_class_count,
        COUNT(DISTINCT CASE WHEN scs.slot_class_section_id IS NOT NULL
              THEN cs.class_section_id END)              AS scheduled_count,
        COUNT(DISTINCT CASE WHEN scs.slot_class_section_id IS NULL
              THEN cs.class_section_id END)              AS unscheduled_count
    FROM {{ ref('dim_class_section') }} cs
    JOIN {{ ref('bridge_child_class_section') }} ccs
        ON ccs.class_section_id = cs.class_section_id AND ccs.is_removed = false
    LEFT JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON scs.class_section_id = cs.class_section_id AND scs.is_removed = false
    WHERE cs.is_active = true AND cs.school_id IS NOT NULL
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

vol_dist_summary AS (
    SELECT
        school_id,
        COUNT(*) FILTER (WHERE vol_count = 1) AS classes_1_vol,
        COUNT(*) FILTER (WHERE vol_count = 2) AS classes_2_vol
    FROM class_volunteer_dist
    GROUP BY school_id
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
    COALESCE(sm.active_child_count, 0)                          AS "Children in Bubble",
    COALESCE(vm.volunteers_recruited, 0)                        AS "Volunteers Recruited",
    COALESCE(vm.volunteers_assigned_to_class, 0)                AS "Volunteers Assigned to Class",
    2                                                           AS "Ideal Slot Count",
    COALESCE(sc.current_slot_count, 0)                          AS "Current Slot Count",
    COALESCE(csm.class_sections_with_children, 0) * 2           AS "Ideal Class Count",
    COALESCE(csm.current_class_count, 0)                        AS "Current Class Count",
    COALESCE(vds.classes_1_vol, 0)                              AS "Classes with 1 Volunteer",
    COALESCE(vds.classes_2_vol, 0)                              AS "Classes with 2 Volunteers",
    COALESCE(csm.unscheduled_count, 0)                          AS "Unscheduled Class Sections",
    COALESCE(csm.unscheduled_count, 0)                          AS "Classes with 0 Volunteers",
    COALESCE(csm.scheduled_count, 0)                            AS "Scheduled Class Sections",
    COALESCE(csm.class_sections_with_children, 0)               AS "Class Sections with Children",
    CASE WHEN COALESCE(csm.class_sections_with_children, 0) > 0
        THEN ROUND((COALESCE(csm.current_class_count, 0) / (csm.class_sections_with_children * 2.0)) * 100, 2)
    END                                                         AS "Percentage Classes vs Ideal Classes",
    CASE WHEN COALESCE(csm.current_class_count, 0) > 0
        THEN ROUND(((COALESCE(vds.classes_1_vol, 0) + COALESCE(vds.classes_2_vol, 0)) / csm.current_class_count::numeric) * 100, 2)
    END                                                         AS "Percentage Classes with At Least 1 Volunteer",
    CASE WHEN COALESCE(csm.current_class_count, 0) > 0
        THEN ROUND((COALESCE(vds.classes_2_vol, 0) / csm.current_class_count::numeric) * 100, 2)
    END                                                         AS "Percentage Classes with 2 Volunteers",
    ROUND(COALESCE(asd.avg_duration, 0), 2)                     AS "Average Slot Duration (Minutes)"

FROM active_partners ap
LEFT JOIN {{ ref('int_bubble__school_metrics') }} sm        ON sm.school_id = ap.partner_id
LEFT JOIN {{ ref('int_bubble__school_volunteer_metrics') }} vm ON vm.school_id = ap.partner_id
LEFT JOIN slot_counts sc                                    ON sc.school_id = ap.partner_id
LEFT JOIN class_scheduling csm                              ON csm.school_id = ap.partner_id
LEFT JOIN vol_dist_summary vds                              ON vds.school_id = ap.partner_id
LEFT JOIN avg_slot_duration asd                             ON asd.school_id = ap.partner_id
ORDER BY ap.partner_name
