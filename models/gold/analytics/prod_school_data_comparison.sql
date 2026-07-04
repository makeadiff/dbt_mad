{{ config(materialized='table') }}

-- School Data Comparison: CRM vs Bubble child count metrics
-- UNION ALL: CRM schools (converted agreements) + Bubble-only schools
-- Optimized: Uses shared int_bubble__school_metrics for child counts

WITH crm_partners AS (
    SELECT
        p.crm_partner_id AS partner_id,
        p.partner_name,
        COALESCE(pco.co_user_id::text, p.created_at::text) AS co_user_id,
        v."UserDisplayName" AS co_name
    FROM {{ ref('dim_crm_partner') }} p
    LEFT JOIN (
        SELECT partner_id, co_id AS co_user_id
        FROM (
            SELECT partner_id, co_id,
                ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY updated_at DESC, created_at DESC, partner_co_id DESC) as rn
            FROM {{ ref('int_crm__partner_cos') }}
        ) ranked WHERE rn = 1
    ) pco ON p.crm_partner_id = pco.partner_id
    LEFT JOIN {{ ref('int_pc_user_data') }} v ON pco.co_user_id::numeric = v."UserId"::numeric
    WHERE p.is_removed = false
),
partner_school_status AS (
    SELECT DISTINCT
        partner_id,
        CASE 
            WHEN latest_stage = 'converted' THEN 'ACTIVE'
            WHEN ever_converted = TRUE THEN 'DROPPED_AFTER_CONVERSION'
            ELSE 'NOT_CONVERTED'
        END AS school_status
    FROM (
        SELECT 
            partner_id,
            FIRST_VALUE(conversion_stage) OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, agreement_id DESC
            ) AS latest_stage,
            BOOL_OR(conversion_stage = 'converted') OVER (
                PARTITION BY partner_id
            ) AS ever_converted
        FROM {{ ref('fct_partner_agreements') }}
    ) ranked
),

latest_mou AS (
    SELECT partner_id, mou_sign_date, confirmed_child_count, mou_status,
        CASE WHEN mou_sign_date IS NOT NULL 
            THEN FLOOR((CURRENT_DATE - mou_sign_date) / 7)
        END AS weeks_since_mou_signed
    FROM (
        SELECT partner_id, mou_sign_date, confirmed_child_count, mou_status, created_at, mou_id,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY created_at DESC, mou_id DESC) as rn
        FROM {{ ref('dim_mou') }}
    ) ranked WHERE rn = 1
),

-- TODO: Integrate Platform Commons (PC) data for dropped child tracking
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

-- CRM schools
-- TODO: Integrate Platform Commons (PC) data for Partner status and child count metrics
SELECT
    p.partner_id::text          AS "Partner ID",
    p.partner_name              AS "Partner Name",
    p.co_user_id                AS "CO ID",
    p.co_name                   AS "CO Name",
    COALESCE(pss.school_status, 'NOT_CONVERTED') AS "School Status",
    m.mou_sign_date             AS "MOU Sign Date",
    m.weeks_since_mou_signed    AS "Weeks Since MOU Signed",
    m.confirmed_child_count     AS "Confirmed Child Count",
    COALESCE(sm.active_child_count, 0)  AS "Active Child Count",
    COALESCE(sm.dropped_child_count, 0) AS "Dropped Child Count",
    COALESCE(adc.count, 0)      AS "Actual Dropped Child Count",
    CASE WHEN COALESCE(sm.active_child_count, 0) > 0 OR COALESCE(sm.dropped_child_count, 0) > 0 
        THEN 'BOTH' ELSE 'CRM' END AS "Platform Presence",
    100                         AS "CRM Status",
    CASE WHEN m.confirmed_child_count > 0 
        THEN ROUND((COALESCE(sm.active_child_count, 0) / m.confirmed_child_count::numeric) * 100, 2)
    END                         AS "Child Count Ratio"
FROM crm_partners p
LEFT JOIN partner_school_status pss ON p.partner_id = pss.partner_id
LEFT JOIN latest_mou m           ON p.partner_id = m.partner_id
LEFT JOIN {{ ref('int_bubble__school_metrics') }} sm ON p.partner_id = sm.school_id
LEFT JOIN actual_dropped_children adc ON p.partner_id = adc.school_id

UNION ALL

-- Bubble-only schools (not in CRM)
-- TODO: Integrate Platform Commons (PC) data to identify unified schools
SELECT
    bp.bubble_partner_id::text  AS "Partner ID",
    bp.partner_name             AS "Partner Name",
    NULL                        AS "CO ID",
    NULL                        AS "CO Name",
    'NOT_CONVERTED'             AS "School Status",
    NULL                        AS "MOU Sign Date",
    NULL                        AS "Weeks Since MOU Signed",
    NULL                        AS "Confirmed Child Count",
    COALESCE(sm.active_child_count, 0)  AS "Active Child Count",
    COALESCE(sm.dropped_child_count, 0) AS "Dropped Child Count",
    COALESCE(adc.count, 0)      AS "Actual Dropped Child Count",
    'BUBBLE'                    AS "Platform Presence",
    0                           AS "CRM Status",
    NULL                        AS "Child Count Ratio"
from {{ ref('dim_bubble_partner') }} bp
left join {{ ref('int_bubble__school_metrics') }} sm ON bp.bubble_partner_id = sm.school_id
left join actual_dropped_children adc ON bp.bubble_partner_id = adc.school_id
left join crm_partners p         ON bp.partner_name = p.partner_name
where p.partner_id IS NULL
  and bp.partner_name IS NOT NULL
  and (COALESCE(sm.active_child_count, 0) > 0 OR COALESCE(sm.dropped_child_count, 0) > 0)
