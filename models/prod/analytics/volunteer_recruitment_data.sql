{{
  config(
    materialized='table',
    schema='analytics',
    description='Volunteer recruitment data with partner details, CO information, confirmed child counts from CRM, volunteer recruitment targets (number of active slots * 2/5 * number of active children), and volunteer assignment metrics'
  )
}}

-- Latest CO assignment per partner from partner_cos_int
WITH latest_partner_cos AS (
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

-- Active partners with latest converted agreement
-- Latest converted agreement per partner
-- Latest partner agreement per partner (latest row regardless of conversion stage)
latest_agreements AS (
    SELECT
        partner_id,
        conversion_stage,
        created_at
    FROM (
        SELECT
            partner_id,
            conversion_stage,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('partner_agreements_int') }}
    ) ranked
    WHERE rn = 1
),

-- Active partners: only partners whose latest partner_agreement conversion_stage = 'converted'
active_partners AS (
    SELECT 
        p.id AS partner_id,
        p.partner_name,
        pco.co_user_id
    FROM {{ ref('partners_int') }} p
    LEFT JOIN latest_partner_cos pco
        ON p.id = pco.partner_id
    JOIN latest_agreements la
        ON p.id::text = la.partner_id::text
       AND la.conversion_stage = 'converted'
    WHERE p.removed = false
),

-- MOU details for each partner with confirmed child count (deduplicated)
mou_details AS (
    SELECT 
        partner_id,
        confirmed_child_count
    FROM (
        SELECT 
            id,
            partner_id,
            confirmed_child_count,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY partner_id 
                ORDER BY created_at DESC, id DESC
            ) as rn
        FROM {{ ref('mous_int') }}
    ) ranked
    WHERE rn = 1
),

-- Community Organizer (CO) details (deduplicated)
co_details AS (
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

-- Volunteer count per school
volunteer_counts AS (
    SELECT 
        sv.school_id,
        COUNT(*) AS volunteer_count
    FROM {{ ref('school_volunteer_int') }} sv
    JOIN {{ ref('partner_int') }} p
        ON sv.school_id = p.partner_id1
    WHERE sv.removed = false
      AND p.removed = false
    GROUP BY sv.school_id
),

-- Volunteers assigned to classes (through slot assignments)
volunteers_assigned_to_class AS (
    SELECT 
        cs.school_id,
        COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned_to_class
    FROM {{ ref('slot_class_section_volunteer_int') }} scsv
    INNER JOIN {{ ref('slot_class_section_int') }} scs
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('class_section_int') }} cs
        ON scs.class_section_id = cs.class_section_id
    WHERE scsv.removed = false
      AND scs.removed = false
      AND cs.removed = false
    GROUP BY cs.school_id
),

-- Active children count per school
active_children AS (
    SELECT 
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false 
      AND is_active = true
    GROUP BY school_id
),

-- Volunteer recruitment target: number of active slots * 2/5 * number of active children (rounded up to integer)
volunteer_recruitment_targets AS (
    SELECT 
        cs.school_id,
        CEIL(COUNT(DISTINCT scs.slot_id) * (2.0 / 5.0) * COALESCE(ac.active_child_count, 0))::integer AS volunteer_recruitment_target
    FROM {{ ref('slot_class_section_int') }} scs
    INNER JOIN {{ ref('class_section_int') }} cs
        ON scs.class_section_id = cs.class_section_id
    LEFT JOIN active_children ac
        ON cs.school_id = ac.school_id
    WHERE scs.removed = false 
      AND scs.is_active = true
    GROUP BY cs.school_id, ac.active_child_count
)

-- Main query combining all data
SELECT
    -- Partner Information
    ap.partner_id::text AS "Partner ID",
    ap.partner_name AS "Partner Name",
    
    -- Community Organizer Details
    cd.co_id AS "CO ID",
    cd.co_name AS "CO Name",
    
    -- Confirmed Child Count from CRM (MOU)
    COALESCE(md.confirmed_child_count, 0) AS "Confirmed Child Count (CRM)",
    
    -- Volunteer Recruitment Target (number of active slots * 2/5 * number of active children)
    COALESCE(vrt.volunteer_recruitment_target, 0) AS "Volunteer Recruitment Target",
    
    -- Ideal Volunteer Recruitment Target (4/5 times confirmed child count)
    CEIL(COALESCE(md.confirmed_child_count, 0) * 4.0 / 5.0) AS "Ideal Volunteer Recruitment Target",
    
    -- Volunteers Recruited
    COALESCE(vc.volunteer_count, 0) AS "Volunteers Recruited",
    
    -- Volunteers Assigned to Class
    COALESCE(vac.volunteers_assigned_to_class, 0) AS "Volunteers Assigned to Class",
    
    -- Percentage Volunteers Assigned to School (Recruited/Target)
    CASE 
        WHEN COALESCE(vrt.volunteer_recruitment_target, 0) > 0 
        THEN ROUND(
            (COALESCE(vc.volunteer_count, 0)::numeric / COALESCE(vrt.volunteer_recruitment_target, 0)::numeric) * 100, 
            5
        )
        ELSE NULL 
    END AS "Percentage Volunteers Assigned to School",
    
    -- Percentage Volunteers Assigned to Class (Assigned to Class/Recruited)
    CASE 
        WHEN COALESCE(vc.volunteer_count, 0) > 0 
        THEN ROUND(
            (COALESCE(vac.volunteers_assigned_to_class, 0)::numeric / COALESCE(vc.volunteer_count, 0)::numeric) * 100, 
            5
        )
        ELSE NULL 
    END AS "Percentage Volunteers Assigned to Class"

FROM active_partners ap

-- Join latest converted agreement
INNER JOIN latest_agreements la
    ON ap.partner_id::text = la.partner_id::text

-- Join MOU details
LEFT JOIN mou_details md 
    ON md.partner_id = ap.partner_id

-- Join CO details
LEFT JOIN co_details cd 
    ON cd.co_id = ap.co_user_id

-- Join volunteer counts (assigned to school)
LEFT JOIN volunteer_counts vc
    ON vc.school_id::text = ap.partner_id

-- Join volunteers assigned to class
LEFT JOIN volunteers_assigned_to_class vac
    ON vac.school_id::text = ap.partner_id

-- Join volunteer recruitment targets
LEFT JOIN volunteer_recruitment_targets vrt
    ON vrt.school_id::text = ap.partner_id

-- Order by partner name
ORDER BY ap.partner_name
