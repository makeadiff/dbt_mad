{{
  config(
    materialized='table',
    schema='analytics',
    description='Class operations data with partner details, CO information, children in bubble count, and volunteer assignment metrics'
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

-- Children in Bubble from Bubble active children (child_int)
mou_details AS (
    SELECT 
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false 
      AND is_active = true
    GROUP BY school_id
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
    WHERE sv.removed = false
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

-- Current slot count per school
current_slot_counts AS (
    SELECT
        school_id,
        COUNT(DISTINCT slot_id) AS current_slot_count
    FROM {{ ref('slot_int') }}
    WHERE removed = false
      AND school_id IS NOT NULL
    GROUP BY school_id
),

-- Current class count per school
current_class_counts AS (
        -- Count scheduled class instances based on slot_class_section rows.
        -- We count distinct slot_class_section_id (each scheduled class occurrence),
        -- joined to class_section to ensure the class_section is active and not removed.
        -- Only count class sections that have at least one child (to match class_sections_with_children logic)
        SELECT
                cs.school_id,
                COUNT(DISTINCT scs.slot_class_section_id) AS current_class_count
        FROM {{ ref('slot_class_section_int') }} scs
        JOIN {{ ref('class_section_int') }} cs
            ON scs.class_section_id = cs.class_section_id
        -- Only include class sections that have at least one child (to match class_sections_with_children)
        JOIN {{ ref('child_class_section_int') }} ccs
            ON ccs.class_section_id = cs.class_section_id
            AND (ccs.removed_boolean IS NULL OR ccs.removed_boolean = false)
        WHERE scs.removed = false
            AND cs.removed = false
            AND cs.is_active = true
            AND cs.school_id IS NOT NULL
        GROUP BY cs.school_id
),

-- Compute volunteer counts per class_section, then aggregate to 0/1/2 counts so sums match current_class_count
class_volunteer_summary AS (
    -- Aggregate volunteers at the slot_class_section (scheduled class) level so counts align with scheduled classes
    -- Only include class sections that have at least one child (to match class_sections_with_children logic)
    SELECT
        cs.school_id,
        scs.slot_class_section_id,
        scs.class_section_id,
        COUNT(DISTINCT scsv.volunteer_id) FILTER (WHERE scsv.removed = false) AS volunteer_count
    FROM {{ ref('slot_class_section_int') }} scs
    JOIN {{ ref('class_section_int') }} cs
      ON scs.class_section_id::text = cs.class_section_id::text
    -- Only include class sections that have at least one child (to match class_sections_with_children)
    JOIN {{ ref('child_class_section_int') }} ccs
      ON ccs.class_section_id::text = cs.class_section_id::text
      AND (ccs.removed_boolean IS NULL OR ccs.removed_boolean = false)
    LEFT JOIN {{ ref('slot_class_section_volunteer_int') }} scsv
      ON scs.slot_class_section_id = scsv.slot_class_section_id
    WHERE scs.removed = false
      AND cs.removed = false
      AND cs.is_active = true
      AND cs.school_id IS NOT NULL
    GROUP BY cs.school_id, scs.slot_class_section_id, scs.class_section_id
),

-- Class sections that have no scheduled slot_class_section rows (unscheduled classes)
unscheduled_class_sections AS (
    -- Deduplicate class_section_id: join to child_class_section_int will create multiple rows
    -- when a class_section has multiple children. We use DISTINCT to ensure we count
    -- unscheduled class sections once per class_section_id.
    SELECT DISTINCT
        cs.school_id,
        cs.class_section_id
    FROM {{ ref('class_section_int') }} cs
    -- Only include class sections that have at least one child
    JOIN {{ ref('child_class_section_int') }} ccs
        ON ccs.class_section_id::text = cs.class_section_id::text
        AND (ccs.removed_boolean IS NULL OR ccs.removed_boolean = false)
    LEFT JOIN {{ ref('slot_class_section_int') }} scs
        ON scs.class_section_id = cs.class_section_id
        AND scs.removed = false
    WHERE cs.removed = false
      AND cs.is_active = true
      AND cs.school_id IS NOT NULL
      AND scs.slot_class_section_id IS NULL
),

-- Classes with 0 volunteers: include scheduled classes with zero volunteers
-- and unscheduled class_sections (no slot_class_section rows)
-- Count unscheduled class sections (created but never scheduled)
unscheduled_class_section_counts AS (
    -- Count distinct class_section_id for unscheduled sections to avoid overcounting
    SELECT school_id, COUNT(DISTINCT class_section_id) AS unscheduled_class_section_count
    FROM unscheduled_class_sections
    GROUP BY school_id
),

classes_with_1_volunteer AS (
    SELECT school_id, COUNT(*) AS classes_with_1_volunteer_count
    FROM class_volunteer_summary cvs
    WHERE cvs.volunteer_count = 1
    GROUP BY school_id
),

classes_with_2_volunteers AS (
    SELECT school_id, COUNT(*) AS classes_with_2_volunteers_count
    FROM class_volunteer_summary cvs
    WHERE cvs.volunteer_count = 2
    GROUP BY school_id
),

-- (removed) Scheduled classes with 0 volunteers: not possible — slot_class_section rows are always created with volunteer(s) per insert rules

    -- Total class_sections that have children (i.e., at least one child in the class_section)
    class_sections_with_children AS (
            SELECT cs.school_id,
                         COUNT(DISTINCT ccs.class_section_id) AS class_sections_with_children_count
            FROM {{ ref('child_class_section_int') }} ccs
            JOIN {{ ref('class_section_int') }} cs
                ON ccs.class_section_id = cs.class_section_id
            WHERE (ccs.removed_boolean IS NULL OR ccs.removed_boolean = false)
                AND cs.removed = false
                AND cs.is_active = true
                AND cs.school_id IS NOT NULL
            GROUP BY cs.school_id
    ),

    -- Scheduled class sections: distinct class_section_id that have at least one slot_class_section row
    -- AND have at least one child (to match class_sections_with_children logic)
    scheduled_class_sections AS (
            SELECT cs.school_id,
                         COUNT(DISTINCT scs.class_section_id) AS scheduled_class_sections_count
            FROM {{ ref('slot_class_section_int') }} scs
            JOIN {{ ref('class_section_int') }} cs
                ON scs.class_section_id = cs.class_section_id
            -- Only include class sections that have at least one child (to match class_sections_with_children)
            JOIN {{ ref('child_class_section_int') }} ccs
                ON ccs.class_section_id = cs.class_section_id
                AND (ccs.removed_boolean IS NULL OR ccs.removed_boolean = false)
            WHERE scs.removed = false
                AND cs.removed = false
                AND cs.is_active = true
                AND cs.school_id IS NOT NULL
            GROUP BY cs.school_id
    ),

-- Average slot duration per school
-- First calculate average duration for each individual slot, then average across all slots
average_slot_duration AS (
    SELECT
        school_id,
        AVG(individual_slot_avg_duration) AS avg_slot_duration_minutes
    FROM (
        SELECT
            school_id,
            slot_id,
            AVG(
                EXTRACT(EPOCH FROM (end_time::timestamp - start_time::timestamp)) / 60
            ) AS individual_slot_avg_duration
        FROM {{ ref('slot_int') }}
        WHERE removed = false
          AND school_id IS NOT NULL
          AND start_time IS NOT NULL
          AND end_time IS NOT NULL
        GROUP BY school_id, slot_id
    ) slot_averages
    GROUP BY school_id
)

-- Main query combining all data
SELECT
    -- Partner Information
    ap.partner_id::text AS "Partner ID",
    ap.partner_name AS "Partner Name",
    
    -- Community Organizer Details
    cd.co_id AS "CO ID",
    cd.co_name AS "CO Name",
    
    -- Children in Bubble (Active children from Bubble)
    COALESCE(md.active_child_count, 0) AS "Children in Bubble",
    
    -- Volunteers Recruited
    COALESCE(vc.volunteer_count, 0) AS "Volunteers Recruited",
    
    -- Volunteers Assigned to Class
    COALESCE(vac.volunteers_assigned_to_class, 0) AS "Volunteers Assigned to Class",
    
    -- Ideal slot count (set to 2 for all partners)
    2 AS "Ideal Slot Count",
    
    -- Current slot count
    COALESCE(csc.current_slot_count, 0) AS "Current Slot Count",
    
    -- Ideal class count: use actual class sections which have children (more accurate than deriving from child counts)
    COALESCE(csc_children.class_sections_with_children_count, 0) * 2 AS "Ideal Class Count",
    
    -- Current class count
    COALESCE(ccc.current_class_count, 0) AS "Current Class Count",
    
    -- Classes with 1 volunteer assigned
    COALESCE(c1v.classes_with_1_volunteer_count, 0) AS "Classes with 1 Volunteer",

    -- Classes with 2 volunteers assigned
    COALESCE(c2v.classes_with_2_volunteers_count, 0) AS "Classes with 2 Volunteers",

    -- Unscheduled class sections (created but never scheduled) for class sections that have children
    COALESCE(c0v.unscheduled_class_section_count, 0) AS "Unscheduled Class Sections",

    -- Classes with 0 volunteers: unscheduled class sections only (created but never scheduled), limited
    -- to class_sections that have children (class_sections_with_children), per operational rules
    COALESCE(c0v.unscheduled_class_section_count, 0) AS "Classes with 0 Volunteers",

    -- Scheduled class sections (distinct class_section_id that have slot_class_section rows)
    COALESCE(scheduled_cs.scheduled_class_sections_count, 0) AS "Scheduled Class Sections",

    -- Class sections with children (class sections that have at least 1 child)
    COALESCE(csc_children.class_sections_with_children_count, 0) AS "Class Sections with Children",
    
    -- Percentage classes vs ideal classes (Current Class Count / Ideal Class Count * 100)
    CASE 
        WHEN COALESCE(csc_children.class_sections_with_children_count, 0) > 0 
        THEN ROUND(
            (COALESCE(ccc.current_class_count, 0)::numeric / (COALESCE(csc_children.class_sections_with_children_count, 0)::numeric * 2)) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes vs Ideal Classes",
    
    -- Percentage classes with at least 1 volunteer ((Classes with 1 + Classes with 2) / Current Class Count * 100)
    CASE 
        WHEN COALESCE(ccc.current_class_count, 0) > 0 
        THEN ROUND(
            ((COALESCE(c1v.classes_with_1_volunteer_count, 0) + COALESCE(c2v.classes_with_2_volunteers_count, 0))::numeric / COALESCE(ccc.current_class_count, 0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes with At Least 1 Volunteer",
    
    -- Percentage classes with 2 volunteers (Classes with 2 / Current Class Count * 100)
    CASE 
        WHEN COALESCE(ccc.current_class_count, 0) > 0 
        THEN ROUND(
            (COALESCE(c2v.classes_with_2_volunteers_count, 0)::numeric / COALESCE(ccc.current_class_count, 0)::numeric) * 100, 
            2
        )
        ELSE NULL 
    END AS "Percentage Classes with 2 Volunteers",
    
    -- Average slot duration in minutes
    ROUND(COALESCE(asd.avg_slot_duration_minutes, 0), 2) AS "Average Slot Duration (Minutes)"

FROM active_partners ap

-- Join latest converted agreement
INNER JOIN latest_agreements la
    ON ap.partner_id::text = la.partner_id::text

-- Join MOU details
LEFT JOIN mou_details md
    ON md.school_id::text = ap.partner_id

-- Join CO details
LEFT JOIN co_details cd
    ON cd.co_id = ap.co_user_id

-- Join volunteer counts (assigned to school)
LEFT JOIN volunteer_counts vc
    ON vc.school_id::text = ap.partner_id

-- Join volunteers assigned to class
LEFT JOIN volunteers_assigned_to_class vac
    ON vac.school_id::text = ap.partner_id

-- Join current slot counts
LEFT JOIN current_slot_counts csc
    ON csc.school_id::text = ap.partner_id

-- Join current class counts
LEFT JOIN current_class_counts ccc
    ON ccc.school_id::text = ap.partner_id

-- Join classes with 1 volunteer
LEFT JOIN classes_with_1_volunteer c1v
    ON c1v.school_id::text = ap.partner_id

-- Join classes with 2 volunteers
LEFT JOIN classes_with_2_volunteers c2v
    ON c2v.school_id::text = ap.partner_id

LEFT JOIN unscheduled_class_section_counts c0v
    ON c0v.school_id::text = ap.partner_id


-- Join scheduled class sections counts
LEFT JOIN scheduled_class_sections scheduled_cs
    ON scheduled_cs.school_id::text = ap.partner_id

-- Join class sections with children counts
LEFT JOIN class_sections_with_children csc_children
    ON csc_children.school_id::text = ap.partner_id

-- Join average slot duration
LEFT JOIN average_slot_duration asd
    ON asd.school_id::text = ap.partner_id

-- Order by partner name
ORDER BY ap.partner_name