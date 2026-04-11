{{
  config(
    materialized='table',
    description='Volunteer recruitment: targets, recruitment status, and assignment metrics per partner'
  )
}}

-- Optimized: Uses shared int models for child/volunteer counts

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

-- Recruitment target: active_slots * 2/5 * active_children
recruitment_targets AS (
    SELECT cs.school_id,
        CEIL(COUNT(DISTINCT scs.slot_id) * (2.0 / 5.0) * COALESCE(sm.active_child_count, 0))::integer AS target
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    INNER JOIN {{ ref('dim_class_section') }} cs ON scs.class_section_id = cs.class_section_id
    LEFT JOIN {{ ref('int_bubble__school_metrics') }} sm ON cs.school_id = sm.school_id
    WHERE scs.is_removed = false AND scs.is_active = true
    GROUP BY cs.school_id, sm.active_child_count
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
    COALESCE(vm.volunteers_recruited, 0)                        AS "Volunteers Recruited",
    COALESCE(vm.volunteers_assigned_to_class, 0)                AS "Volunteers Assigned to Class",
    CASE WHEN COALESCE(rt.target, 0) > 0
        THEN ROUND((COALESCE(vm.volunteers_recruited, 0) / rt.target::numeric) * 100, 5)
    END                                                         AS "Percentage Volunteers Assigned to School",
    CASE WHEN COALESCE(vm.volunteers_recruited, 0) > 0
        THEN ROUND((COALESCE(vm.volunteers_assigned_to_class, 0) / vm.volunteers_recruited::numeric) * 100, 5)
    END                                                         AS "Percentage Volunteers Assigned to Class"

FROM active_partners ap
LEFT JOIN latest_mou lm                                     ON lm.partner_id = ap.partner_id
LEFT JOIN {{ ref('int_bubble__school_volunteer_metrics') }} vm ON vm.school_id = ap.partner_id
LEFT JOIN recruitment_targets rt                            ON rt.school_id = ap.partner_id
ORDER BY ap.partner_name
