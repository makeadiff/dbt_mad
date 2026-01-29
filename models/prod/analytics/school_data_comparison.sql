{{ config(materialized='table') }}
-- School Data Comparison: Partner schools with CO details and child count metrics
-- Updated to implement partner lifecycle status (Option 1)
--
-- School Status Definitions:
-- - ACTIVE: Partner was converted AND latest agreement is still 'converted' AND partner not removed
-- - DROPPED_AFTER_CONVERSION: Partner was once converted (ever_converted=1) AND latest_conversion_stage='dropped'
--   AND non_conversion_reason IN ('school_dropped', 'school_inactive', 'school_did_not_want_to_continue_with_mad')
-- - NOT_CONVERTED: Partner exists in CRM but doesn't meet ACTIVE or DROPPED_AFTER_CONVERSION criteria
-- - BUBBLE_ONLY: Partner exists only in Bubble platform, not in CRM
--
-- Assumptions:
-- - ever_converted: Partner has ANY agreement row where conversion_stage = 'converted'
--   (we ignore agreement.removed flag for ever_converted to capture historical conversions)
-- - For active status, we check latest agreement.removed IS FALSE OR NULL (treating NULL as not removed)
-- - partner.removed = true means the partner record itself is removed/archived

WITH latest_partner_cos AS (
    -- Latest CO assignment per partner from partner_cos_int
    SELECT
        partner_id,
        co_id AS co_user_id
    FROM (
        SELECT
            partner_id,
            co_id,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id
                ORDER BY updated_at DESC, created_at DESC, id DESC
            ) as rn
        FROM {{ ref('partner_cos_int') }}
    ) ranked
    WHERE rn = 1
),

partners AS (
    -- Base partner data with CO from partner_cos_int
    -- NOTE: Removed the WHERE p.removed = false filter to include all partners
    -- This allows us to track partners that were once active but later removed
    SELECT
        p.id AS partner_id,
        p.partner_name,
        COALESCE(pco.co_user_id, p.created_by) AS co_user_id,
        p.removed AS partner_removed
    FROM {{ ref('partners_int') }} p
    LEFT JOIN latest_partner_cos pco
        ON p.id = pco.partner_id
),

latest_agreement_per_partner AS (
    -- Latest agreement per partner (no filtering by conversion_stage)
    -- This gives us the current state of each partner's agreement
    SELECT
        partner_id,
        conversion_stage AS latest_conversion_stage,
        non_conversion_reason,
        removed AS agreement_removed,
        created_at
    FROM (
        SELECT
            id,
            partner_id,
            conversion_stage,
            non_conversion_reason,
            removed,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('partner_agreements_int') }}
    ) ranked
    WHERE rn = 1
),

ever_converted_per_partner AS (
    -- Check if partner has EVER been converted (any agreement with conversion_stage = 'converted')
    -- We don't filter by removed here because we want to know historical conversion status
    -- even if that specific agreement was later removed
    SELECT
        partner_id,
        1 AS ever_converted
    FROM {{ ref('partner_agreements_int') }}
    WHERE conversion_stage = 'converted'
    GROUP BY partner_id
),

mou_data AS (
    -- MOU details with confirmed child count (deduplicated)
    SELECT
        partner_id,
        mou_sign_date,
        confirmed_child_count,
        weeks_since_mou_signed
    FROM (
        SELECT
            id,
            partner_id,
            mou_sign_date,
            confirmed_child_count,
            CASE
                WHEN mou_sign_date IS NOT NULL
                THEN FLOOR((CURRENT_DATE - mou_sign_date::date)::numeric / 7)::integer
                ELSE NULL
            END AS weeks_since_mou_signed,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('mous_int') }}
    ) ranked
    WHERE rn = 1
),

community_organizers AS (
    -- Community Organizer details (deduplicated)
    SELECT
        user_id AS co_id,
        user_display_name AS co_name
    FROM (
        SELECT
            user_id,
            user_display_name,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY user_updated_datetime DESC, user_created_datetime DESC
            ) as rn
        FROM {{ ref('user_data_int') }}
    ) ranked
    WHERE rn = 1
),

active_children AS (
    -- Active children count from Bubble
    SELECT
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false
      AND is_active = true
    GROUP BY school_id
),

dropped_children AS (
    -- Dropped children count from Bubble
    SELECT
        school_id,
        COUNT(*) AS dropped_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false
      AND is_active = false
    GROUP BY school_id
),

actual_dropped_children AS (
    -- Actual dropped child count based on removal_reason from child_removal_log_int
    SELECT
        school_id,
        COUNT(*) AS actual_dropped_child_count
    FROM {{ ref('child_removal_log_int') }}
    WHERE removed = false
      AND (
            removal_reason IN (
                'Transferred to another school',
                'Dropped out of school',
                'Family does not want the child enrolled',
                'Child no longer interested in participating',
                'Inactive',
                'school dropped from crm'
            )
            OR (
                removal_reason = 'Other'
                AND LOWER(TRIM(other_details)) = 'school dropped from crm'
            )
          )
    GROUP BY school_id
),

-- Compute school status for CRM partners
crm_partners_with_status AS (
    SELECT
        p.partner_id,
        p.partner_name,
        p.co_user_id,
        p.partner_removed,
        la.latest_conversion_stage,
        la.non_conversion_reason,
        la.agreement_removed,
        COALESCE(ec.ever_converted, 0) AS ever_converted,
        -- School Status Logic:
        -- ACTIVE: ever_converted AND latest_stage='converted' AND partner not removed AND agreement not removed
        -- DROPPED_AFTER_CONVERSION: ever_converted AND latest_stage='dropped' AND specific non_conversion_reasons
        -- NOT_CONVERTED: everything else (never converted or doesn't meet dropped criteria)
        CASE
            WHEN COALESCE(ec.ever_converted, 0) = 1
                 AND la.latest_conversion_stage = 'converted'
                 AND p.partner_removed = false
                 AND (la.agreement_removed = false OR la.agreement_removed IS NULL)
            THEN 'ACTIVE'
            WHEN COALESCE(ec.ever_converted, 0) = 1
                 AND la.latest_conversion_stage = 'dropped'
                 AND la.non_conversion_reason IN (
                     'school_dropped',
                     'school_inactive',
                     'school_did_not_want_to_continue_with_mad'
                 )
            THEN 'DROPPED_AFTER_CONVERSION'
            ELSE 'NOT_CONVERTED'
        END AS school_status,
        -- Is Active School (1 only for ACTIVE status)
        CASE
            WHEN COALESCE(ec.ever_converted, 0) = 1
                 AND la.latest_conversion_stage = 'converted'
                 AND p.partner_removed = false
                 AND (la.agreement_removed = false OR la.agreement_removed IS NULL)
            THEN 1
            ELSE 0
        END AS is_active_school
    FROM partners p
    LEFT JOIN latest_agreement_per_partner la
        ON p.partner_id::text = la.partner_id::text
    LEFT JOIN ever_converted_per_partner ec
        ON p.partner_id::text = ec.partner_id::text
)

-- Schools in CRM (all partners with lifecycle status)
SELECT
    -- Partner Information
    cps.partner_id::text AS "Partner ID",
    cps.partner_name AS "Partner Name",

    -- Community Organizer Details
    co.co_id AS "CO ID",
    co.co_name AS "CO Name",

    -- MOU Information
    m.mou_sign_date AS "MOU Sign Date",
    m.weeks_since_mou_signed AS "Weeks Since MOU Signed",

    -- Child Count Metrics
    m.confirmed_child_count AS "Confirmed Child Count (CRM)",
    COALESCE(ac.active_child_count, 0) AS "Active Child Count (Bubble)",
    COALESCE(dc.dropped_child_count, 0) AS "Dropped Child Count (Bubble)",
    COALESCE(adc.actual_dropped_child_count, 0) AS "Actual Dropped Child Count (Bubble)",

    -- Platform Presence (now includes dropped partners too)
    CASE
        WHEN ac.active_child_count > 0 OR dc.dropped_child_count > 0
        THEN 'BOTH'
        ELSE 'CRM'
    END AS "Platform Presence",

    -- CRM Status (numeric indicator)
    100 AS "CRM Status",

    -- Child Count Ratio (Bubble / CRM)
    CASE
        WHEN m.confirmed_child_count > 0
        THEN ROUND(
            (COALESCE(ac.active_child_count, 0)::numeric / m.confirmed_child_count::numeric) * 100,
            2
        )
        ELSE NULL
    END AS "Child Count Ratio (Bubble / CRM)",

    -- NEW: Lifecycle Status Columns
    cps.school_status AS "School Status",
    cps.is_active_school AS "Is Active School",
    cps.latest_conversion_stage AS "Latest Conversion Stage",
    cps.non_conversion_reason AS "Non Conversion Reason",
    cps.ever_converted AS "Ever Converted"

FROM crm_partners_with_status cps

-- Join with MOU data
LEFT JOIN mou_data m
    ON cps.partner_id::text = m.partner_id::text

-- Join with Community Organizer details
LEFT JOIN community_organizers co
    ON cps.co_user_id::text = co.co_id::text

-- Join with active children count
LEFT JOIN active_children ac
    ON cps.partner_id::numeric = ac.school_id::numeric

-- Join with dropped children count
LEFT JOIN dropped_children dc
    ON cps.partner_id::numeric = dc.school_id::numeric

-- Join with actual dropped children count
LEFT JOIN actual_dropped_children adc
    ON cps.partner_id::numeric = adc.school_id::numeric

UNION ALL

-- Schools in Bubble but not in CRM
SELECT
    -- Partner Information (use school ID and name from Bubble)
    bp.partner_id1::integer::text AS "Partner ID",
    bp.partner_name AS "Partner Name",

    -- Community Organizer Details (NULL for Bubble-only schools)
    NULL AS "CO ID",
    NULL AS "CO Name",

    -- MOU Information (NULL for Bubble-only schools)
    NULL AS "MOU Sign Date",
    NULL AS "Weeks Since MOU Signed",

    -- Child Count Metrics
    NULL AS "Confirmed Child Count (CRM)",
    COALESCE(ac.active_child_count, 0) AS "Active Child Count (Bubble)",
    COALESCE(dc.dropped_child_count, 0) AS "Dropped Child Count (Bubble)",
    COALESCE(adc.actual_dropped_child_count, 0) AS "Actual Dropped Child Count (Bubble)",

    -- Platform Presence
    'BUBBLE' AS "Platform Presence",

    -- CRM Status (numeric indicator)
    0 AS "CRM Status",

    -- Child Count Ratio (Bubble / CRM) - NULL for Bubble-only schools
    NULL AS "Child Count Ratio (Bubble / CRM)",

    -- NEW: Lifecycle Status Columns for Bubble-only
    'BUBBLE_ONLY' AS "School Status",
    0 AS "Is Active School",
    NULL AS "Latest Conversion Stage",
    NULL AS "Non Conversion Reason",
    0 AS "Ever Converted"

FROM {{ ref('partner_int') }} bp

-- Join with active children count
LEFT JOIN active_children ac
    ON bp.partner_id1::numeric = ac.school_id::numeric

-- Join with dropped children count
LEFT JOIN dropped_children dc
    ON bp.partner_id1::numeric = dc.school_id::numeric

-- Join with actual dropped children count
LEFT JOIN actual_dropped_children adc
    ON bp.partner_id1::numeric = adc.school_id::numeric

-- Exclude schools that are already in CRM (by partner_id match)
LEFT JOIN partners p
    ON bp.partner_id1::integer::text = p.partner_id::text

WHERE p.partner_id IS NULL  -- Only include schools not in CRM
  AND bp.partner_name IS NOT NULL
  AND bp.removed = false  -- Only include non-removed Bubble partners
  AND (ac.active_child_count > 0 OR dc.dropped_child_count > 0)  -- Only include schools with children
