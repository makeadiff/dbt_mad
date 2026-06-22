{{
  config(
    materialized='table',
    description='SRIC dashboard data (E2 only) — chapter mapping enriched with Bubble child/volunteer counts and live recruitment/sourcing targets'
  )
}}

-- Source: {{ ref('master_mapping_sheet_int') }}
-- Grain: one row per E2 master mapping sheet row (chapter_id + cho_id)
WITH master_mapping_sheet_int__chapters AS (
    SELECT
        city_name,
        state,
        co_id,
        co_name,
        chapter_id,
        chapter_name,
        worknode_id,
        cho_id,
        cho_name,
        engine,
        chapter_status,
        sourcing_campaign_code
    FROM {{ ref('master_mapping_sheet_int') }}
    WHERE engine = 'E2'
),

-- Source: {{ ref('partners_int') }}
-- Grain: one row per E2 chapter_id + school_id (1:1 — chapter_id equals CRM/Bubble school id)
partners_int__e2_schools_by_chapter AS (
    SELECT DISTINCT
        m.chapter_id,
        p.id::integer AS school_id
    FROM master_mapping_sheet_int__chapters m
    INNER JOIN {{ ref('partners_int') }} p
        ON m.chapter_id::integer = p.id::integer
    WHERE p.removed = false
),

-- Source: {{ ref('child_int') }}
-- Grain: one row per school_id
child_int__active_children_per_school AS (
    SELECT
        school_id,
        COUNT(*) AS active_child_count
    FROM {{ ref('child_int') }}
    WHERE removed = false
      AND is_active = true
    GROUP BY school_id
),

-- Source: {{ ref('school_volunteer_int') }}, {{ ref('partner_int') }}
-- Grain: one row per school_id
school_volunteer_int__volunteer_count_per_school AS (
    SELECT
        sv.school_id,
        COUNT(*) AS volunteer_count
    FROM {{ ref('school_volunteer_int') }} sv
    INNER JOIN {{ ref('partner_int') }} p
        ON sv.school_id = p.partner_id1
    WHERE sv.removed = false
      AND p.removed = false
    GROUP BY sv.school_id
),

-- Source: {{ ref('slot_class_section_volunteer_int') }}, {{ ref('slot_class_section_int') }}, {{ ref('class_section_int') }}
-- Grain: one row per school_id
slot_class_section_int__volunteers_assigned_to_class_per_school AS (
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

-- Source: {{ ref('slot_class_section_int') }}, {{ ref('class_section_int') }}
-- Grain: one row per school_id
-- Active slot_class_section rows used in recruitment target calculation
slot_class_section_int__active_slot_counts_per_school AS (
    SELECT
        cs.school_id,
        COUNT(DISTINCT scs.slot_id) AS active_slot_count,
        COUNT(DISTINCT scs.slot_class_section_id) AS active_slot_class_section_count
    FROM {{ ref('slot_class_section_int') }} scs
    INNER JOIN {{ ref('class_section_int') }} cs
        ON scs.class_section_id = cs.class_section_id
    WHERE scs.removed = false
      AND scs.is_active = true
      AND cs.removed = false
    GROUP BY cs.school_id
),

-- Source: slot_class_section_int__active_slot_counts_per_school
-- Grain: one row per school_id
-- Formula: active_slot_count * active_slot_class_section_count * 2
slot_class_section_int__volunteer_recruitment_target_per_school AS (
    SELECT
        sc.school_id,
        sc.active_slot_count,
        sc.active_slot_class_section_count,
        (sc.active_slot_count * sc.active_slot_class_section_count * 2)::integer AS volunteer_recruitment_target
    FROM slot_class_section_int__active_slot_counts_per_school sc
),

-- Source: partners_int__e2_schools_by_chapter + school-level Bubble metrics
-- Grain: one row per E2 chapter_id
partners_int__e2_bubble_metrics_by_chapter AS (
    SELECT
        esc.chapter_id,
        COALESCE(SUM(ac.active_child_count), 0) AS active_child_count,
        COALESCE(SUM(vrt.active_slot_count), 0) AS active_slot_count,
        COALESCE(SUM(vrt.active_slot_class_section_count), 0) AS active_slot_class_section_count,
        COALESCE(SUM(vc.volunteer_count), 0) AS volunteer_count,
        COALESCE(SUM(vac.volunteers_assigned_to_class), 0) AS volunteers_assigned_to_class,
        COALESCE(SUM(vrt.volunteer_recruitment_target), 0) AS volunteer_recruitment_target
    FROM partners_int__e2_schools_by_chapter esc
    LEFT JOIN child_int__active_children_per_school ac
        ON esc.school_id = ac.school_id
    LEFT JOIN school_volunteer_int__volunteer_count_per_school vc
        ON esc.school_id = vc.school_id
    LEFT JOIN slot_class_section_int__volunteers_assigned_to_class_per_school vac
        ON esc.school_id = vac.school_id
    LEFT JOIN slot_class_section_int__volunteer_recruitment_target_per_school vrt
        ON esc.school_id = vrt.school_id
    GROUP BY esc.chapter_id
),

final AS (
    SELECT
        ch.city_name,
        ch.state,
        ch.co_id,
        ch.co_name,
        ch.chapter_id,
        ch.chapter_name,
        ch.worknode_id,
        ch.cho_id,
        ch.cho_name,
        ch.engine,
        ch.chapter_status,
        ch.sourcing_campaign_code,
        COALESCE(bm.active_child_count, 0) AS active_child_count,
        COALESCE(bm.active_slot_count, 0) AS active_slot_count,
        COALESCE(bm.active_slot_class_section_count, 0) AS active_slot_class_section_count,
        COALESCE(bm.volunteer_count, 0) AS volunteer_count,
        COALESCE(bm.volunteers_assigned_to_class, 0) AS volunteers_assigned_to_class,
        COALESCE(bm.volunteer_recruitment_target, 0) AS volunteer_recruitment_target,
        COALESCE(bm.volunteer_recruitment_target, 0) * 3 AS sourcing_target,
        GREATEST(
            0,
            COALESCE(bm.volunteer_recruitment_target, 0) - COALESCE(bm.volunteer_count, 0)
        ) AS live_volunteer_recruitment_target,
        GREATEST(
            0,
            COALESCE(bm.volunteer_recruitment_target, 0) - COALESCE(bm.volunteer_count, 0)
        ) * 3 AS live_sourcing_target
    FROM master_mapping_sheet_int__chapters ch
    LEFT JOIN partners_int__e2_bubble_metrics_by_chapter bm
        ON ch.chapter_id = bm.chapter_id
)

SELECT * FROM final
