{{ config(materialized='table') }}

-- School Data Comparison: CRM vs Bubble child count metrics
-- UNION ALL: CRM schools (converted agreements) + Bubble-only schools

WITH crm_partners AS (
    SELECT
        p.crm_partner_id AS partner_id,
        p.partner_name,
        COALESCE(pco.co_user_id::text, p.created_at::text) AS co_user_id,
        v.volunteer_name AS co_name
    FROM {{ ref('dim_crm_partner') }} p
    LEFT JOIN (
        SELECT partner_id, co_id AS co_user_id
        FROM (
            SELECT partner_id, co_id,
                ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY updated_at DESC, created_at DESC, partner_co_id DESC) as rn
            FROM {{ ref('int_crm__partner_cos') }}
        ) ranked WHERE rn = 1
    ) pco ON p.crm_partner_id = pco.partner_id
    LEFT JOIN {{ ref('dim_volunteer') }} v ON pco.co_user_id = v.volunteer_id
    WHERE p.is_removed = false
),

converted_partners AS (
    SELECT partner_id
    FROM (
        SELECT partner_id, conversion_stage,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, agreement_id DESC) as rn
        FROM {{ ref('fct_partner_agreements') }}
    ) ranked
    WHERE rn = 1 AND conversion_stage = 'converted'
),

latest_mou AS (
    SELECT partner_id, mou_sign_date, confirmed_child_count,
        CASE WHEN mou_sign_date IS NOT NULL 
            THEN FLOOR((CURRENT_DATE - mou_sign_date) / 7)
        END AS weeks_since_mou_signed
    FROM (
        SELECT partner_id, mou_sign_date, confirmed_child_count, created_at, mou_id,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, mou_id DESC) as rn
        FROM {{ ref('dim_mou') }}
    ) ranked WHERE rn = 1
),

-- TODO: Integrate Platform Commons (PC) data for Active Child tracking
active_children AS (
    SELECT school_id, COUNT(*) AS count FROM {{ ref('dim_child') }} WHERE is_active = true GROUP BY school_id
),

-- TODO: Integrate Platform Commons (PC) data for Dropped Child tracking
dropped_children AS (
    SELECT school_id, COUNT(*) AS count FROM {{ ref('dim_child') }} WHERE is_active = false GROUP BY school_id
),

actual_dropped_children AS (
    SELECT school_id, COUNT(*) AS count
    FROM {{ ref('int_bubble__child_removal_log') }}
    WHERE is_removed = false
      AND removal_reason IN (
          'Transferred to another school', 'Dropped out of school',
          'Family does not want the child enrolled',
          'Child no longer interested in participating', 'Inactive'
      )
    GROUP BY school_id
)

-- CRM schools with converted agreements
-- TODO: Integrate Platform Commons (PC) data for Partner status and child count metrics
SELECT
    p.partner_id                AS "Partner ID",
    p.partner_name              AS "Partner Name",
    p.co_user_id                AS "CO ID",
    p.co_name                   AS "CO Name",
    m.mou_sign_date             AS "MOU Sign Date",
    m.weeks_since_mou_signed    AS "Weeks Since MOU Signed",
    m.confirmed_child_count     AS "Confirmed Child Count (CRM)",
    COALESCE(ac.count, 0)       AS "Active Child Count (Bubble)",
    COALESCE(dc.count, 0)       AS "Dropped Child Count (Bubble)",
    COALESCE(adc.count, 0)      AS "Actual Dropped Child Count (Bubble)",
    CASE WHEN ac.count > 0 OR dc.count > 0 THEN 'BOTH' ELSE 'CRM' END AS "Platform Presence",
    100                         AS "CRM Status",
    CASE WHEN m.confirmed_child_count > 0 
        THEN ROUND((COALESCE(ac.count, 0) / m.confirmed_child_count::numeric) * 100, 2)
    END                         AS "Child Count Ratio (Bubble / CRM)"
FROM crm_partners p
INNER JOIN converted_partners cp ON p.partner_id = cp.partner_id
LEFT JOIN latest_mou m           ON p.partner_id = m.partner_id
LEFT JOIN active_children ac     ON p.partner_id = ac.school_id
LEFT JOIN dropped_children dc    ON p.partner_id = dc.school_id
LEFT JOIN actual_dropped_children adc ON p.partner_id = adc.school_id

UNION ALL

-- Bubble-only schools (not in CRM)
-- TODO: Integrate Platform Commons (PC) data to identify unified schools
SELECT
    bp.bubble_partner_id        AS "Partner ID",
    bp.partner_name             AS "Partner Name",
    NULL                        AS "CO ID",
    NULL                        AS "CO Name",
    NULL                        AS "MOU Sign Date",
    NULL                        AS "Weeks Since MOU Signed",
    NULL                        AS "Confirmed Child Count (CRM)",
    COALESCE(ac.count, 0)       AS "Active Child Count (Bubble)",
    COALESCE(dc.count, 0)       AS "Dropped Child Count (Bubble)",
    COALESCE(adc.count, 0)      AS "Actual Dropped Child Count (Bubble)",
    'BUBBLE'                    AS "Platform Presence",
    0                           AS "CRM Status",
    NULL                        AS "Child Count Ratio (Bubble / CRM)"
FROM {{ ref('dim_bubble_partner') }} bp
LEFT JOIN active_children ac     ON bp.bubble_partner_id = ac.school_id
LEFT JOIN dropped_children dc    ON bp.bubble_partner_id = dc.school_id
LEFT JOIN actual_dropped_children adc ON bp.bubble_partner_id = adc.school_id
LEFT JOIN crm_partners p         ON bp.partner_name = p.partner_name
WHERE p.partner_id IS NULL
  AND bp.partner_name IS NOT NULL
  AND (ac.count > 0 OR dc.count > 0)
