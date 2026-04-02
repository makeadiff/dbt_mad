{{
  config(
    materialized='table',
    description='Volunteer recruitment: targets, recruitment status, and assignment metrics per partner'
  )
}}

WITH active_partners AS (
    SELECT * FROM {{ ref('int_crm__active_partners') }}
),

-- Latest MOU per partner for confirmed child count
latest_mou AS (
    SELECT partner_id, confirmed_child_count
    FROM (
        SELECT partner_id, confirmed_child_count,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, mou_id DESC) as rn
        FROM {{ ref('dim_mou') }}
    ) ranked
    WHERE rn = 1
),

-- Volunteers recruited per school
volunteers_per_school AS (
    SELECT school_id, COUNT(*) AS volunteer_count
    FROM {{ ref('fct_school_volunteer') }}
    WHERE is_removed = false
    GROUP BY school_id
),

-- Volunteers assigned to classes
volunteers_in_class AS (
    SELECT cs.school_id, COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned
    FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    WHERE scsv.is_removed = false AND scs.is_removed = false
    GROUP BY cs.school_id
),

-- Active children per school
active_children AS (
    SELECT school_id, COUNT(*) AS active_child_count
    FROM {{ ref('dim_child') }}
    WHERE is_active = true
    GROUP BY school_id
),

-- Recruitment target: active_slots * 2/5 * active_children
recruitment_targets AS (
    SELECT cs.school_id,
        CEIL(COUNT(DISTINCT scs.slot_id) * (2.0 / 5.0) * COALESCE(ac.active_child_count, 0))::integer AS target
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    INNER JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    LEFT JOIN active_children ac ON cs.school_id = ac.school_id
    WHERE scs.is_removed = false AND scs.is_active = true
    GROUP BY cs.school_id, ac.active_child_count
)

SELECT
    ap.partner_id                                               AS "Partner ID",
    ap.partner_name                                             AS "Partner Name",
    ap.co_id_numeric                                            AS "CO ID",
    ap.co_name                                                  AS "CO Name",
    -- TODO: Integrate Platform Commons (PC) data for Partner status and child count metrics
    COALESCE(lm.confirmed_child_count, 0)                       AS "Confirmed Child Count (CRM)",
    -- TODO: Integrate Platform Commons (PC) data for Volunteer Recruitment Targets
    COALESCE(rt.target, 0)                                      AS "Volunteer Recruitment Target",
    CEIL(COALESCE(lm.confirmed_child_count, 0) * 4.0 / 5.0)    AS "Ideal Volunteer Recruitment Target",
    -- TODO: Integrate Platform Commons (PC) data for Volunteer Recruitment tracking
    COALESCE(vps.volunteer_count, 0)                             AS "Volunteers Recruited",
    COALESCE(vic.volunteers_assigned, 0)                         AS "Volunteers Assigned to Class",
    CASE WHEN COALESCE(rt.target, 0) > 0
        THEN ROUND((COALESCE(vps.volunteer_count, 0) / rt.target::numeric) * 100, 5)
    END                                                         AS "Percentage Volunteers Assigned to School",
    CASE WHEN COALESCE(vps.volunteer_count, 0) > 0
        THEN ROUND((COALESCE(vic.volunteers_assigned, 0) / vps.volunteer_count::numeric) * 100, 5)
    END                                                         AS "Percentage Volunteers Assigned to Class"

FROM active_partners ap
LEFT JOIN latest_mou lm              ON lm.partner_id = ap.partner_id
LEFT JOIN volunteers_per_school vps  ON vps.school_id = ap.partner_id
LEFT JOIN volunteers_in_class vic    ON vic.school_id = ap.partner_id
LEFT JOIN recruitment_targets rt     ON rt.school_id = ap.partner_id
ORDER BY ap.partner_name
