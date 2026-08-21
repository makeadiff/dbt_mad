{{
  config(
    materialized='table',
    description='SRIC dashboard data (E2 only) — chapter mapping enriched with Bubble child/volunteer counts and live recruitment/sourcing targets'
  )
}}

-- SRIC dashboard: E2 chapter mapping enriched with Bubble child/volunteer counts, CPP/COC
-- signing status, and live recruitment/sourcing targets.
-- Reaches into silver (int_google_sheet__chapter_mapping, int_bubble__slot_class_section,
-- int_pc_applicant_policy_status) rather than marts-only, because no existing dim/fact carries
-- co_id/cho_id/sourcing_campaign_code alongside the chapter mapping, and no existing dim/fact
-- resolves per-volunteer CPP/COC status -- same known deviation as prod_volunteer_recruitment.
-- volunteers_signed_cpp_coc / volunteers_signed_cpp_coc_assigned_to_class treat a volunteer as
-- "signed" only when both CPP and COC are accepted on their latest application (see
-- int_pc_applicant_policy_status's header for the unverified truthy-value assumption).
-- Grain: one row per E2 master mapping sheet row (chapter_id + cho_id)

WITH e2_chapters AS (
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
    FROM {{ ref('int_google_sheet__chapter_mapping') }}
    WHERE engine = 'E2'
),

-- Grain: one row per E2 chapter_id + school_id (1:1 -- chapter_id equals the Bubble partner id)
e2_schools_by_chapter AS (
    SELECT DISTINCT
        m.chapter_id,
        p.bubble_partner_id AS school_id
    FROM e2_chapters m
    INNER JOIN {{ ref('dim_bubble_partner') }} p
        ON m.chapter_id::integer = p.bubble_partner_id::integer
),

-- Grain: one row per school_id
-- Active slot_class_section rows used in the recruitment target calculation
active_slot_counts_per_school AS (
    SELECT
        cs.school_id,
        COUNT(DISTINCT scs.slot_id) AS active_slot_count,
        COUNT(DISTINCT scs.slot_class_section_id) AS active_slot_class_section_count
    FROM {{ ref('int_bubble__slot_class_section') }} scs
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON scs.class_section_id = cs.class_section_id
    WHERE scs.is_removed = false
      AND scs.is_active = true
    GROUP BY cs.school_id
),

-- Formula: active_slot_count * active_slot_class_section_count * 2
volunteer_recruitment_target_per_school AS (
    SELECT
        school_id,
        active_slot_count,
        active_slot_class_section_count,
        (active_slot_count * active_slot_class_section_count * 2)::integer AS volunteer_recruitment_target
    FROM active_slot_counts_per_school
),

-- Grain: one row per E2 chapter_id + recruited volunteer_id
-- fct_school_volunteer already filters is_removed = false.
e2_recruited_volunteers AS (
    SELECT
        esc.chapter_id,
        fsv.volunteer_id
    FROM e2_schools_by_chapter esc
    INNER JOIN {{ ref('fct_school_volunteer') }} fsv
        ON esc.school_id = fsv.school_id
),

-- Grain: one row per E2 chapter_id + volunteer_id assigned to a class section
-- fct_volunteer_slot_assignment already filters is_removed = false.
e2_assigned_volunteers AS (
    SELECT DISTINCT
        esc.chapter_id,
        fvsa.volunteer_id
    FROM e2_schools_by_chapter esc
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON cs.school_id = esc.school_id
    INNER JOIN {{ ref('fct_volunteer_slot_assignment') }} fvsa
        ON fvsa.class_section_id = cs.class_section_id
),

-- CPP/COC signed status per volunteer. Bubble volunteer_id and Platform Commons user_id share the
-- same numeric identity space -- same join used by prod_school_volunteer.sql and
-- prod_volunteer_class_child_view.sql.
e2_recruited_volunteers_with_policy AS (
    SELECT
        rv.chapter_id,
        rv.volunteer_id,
        COALESCE(pol.cpp_accepted, false) AND COALESCE(pol.coc_accepted, false) AS signed_cpp_and_coc
    FROM e2_recruited_volunteers rv
    LEFT JOIN {{ ref('int_pc_applicant_policy_status') }} pol
        ON rv.volunteer_id::numeric = pol.user_id::numeric
),

e2_assigned_volunteers_with_policy AS (
    SELECT
        av.chapter_id,
        av.volunteer_id,
        COALESCE(pol.cpp_accepted, false) AND COALESCE(pol.coc_accepted, false) AS signed_cpp_and_coc
    FROM e2_assigned_volunteers av
    LEFT JOIN {{ ref('int_pc_applicant_policy_status') }} pol
        ON av.volunteer_id::numeric = pol.user_id::numeric
),

-- Grain: one row per E2 chapter_id
e2_cpp_coc_counts_by_chapter AS (
    SELECT
        chapter_id,
        COUNT(DISTINCT volunteer_id) FILTER (WHERE signed_cpp_and_coc) AS volunteers_signed_cpp_coc
    FROM e2_recruited_volunteers_with_policy
    GROUP BY chapter_id
),

-- Grain: one row per E2 chapter_id
e2_cpp_coc_assigned_counts_by_chapter AS (
    SELECT
        chapter_id,
        COUNT(DISTINCT volunteer_id) FILTER (WHERE signed_cpp_and_coc) AS volunteers_signed_cpp_coc_assigned_to_class
    FROM e2_assigned_volunteers_with_policy
    GROUP BY chapter_id
),

-- Grain: one row per E2 chapter_id
-- active_child_count from int_bubble__school_metrics; volunteer_count/volunteers_assigned_to_class
-- from int_bubble__school_volunteer_metrics -- both already shared across other prod_* models,
-- so reused here rather than re-deriving the same rollups.
e2_bubble_metrics_by_chapter AS (
    SELECT
        esc.chapter_id,
        COALESCE(SUM(sm.active_child_count), 0) AS active_child_count,
        COALESCE(SUM(vrt.active_slot_count), 0) AS active_slot_count,
        COALESCE(SUM(vrt.active_slot_class_section_count), 0) AS active_slot_class_section_count,
        COALESCE(SUM(svm.volunteers_recruited), 0) AS volunteer_count,
        COALESCE(SUM(svm.volunteers_assigned_to_class), 0) AS volunteers_assigned_to_class,
        COALESCE(SUM(vrt.volunteer_recruitment_target), 0) AS volunteer_recruitment_target,
        COALESCE(SUM(cpp.volunteers_signed_cpp_coc), 0) AS volunteers_signed_cpp_coc,
        COALESCE(SUM(acpp.volunteers_signed_cpp_coc_assigned_to_class), 0) AS volunteers_signed_cpp_coc_assigned_to_class
    FROM e2_schools_by_chapter esc
    LEFT JOIN {{ ref('int_bubble__school_metrics') }} sm
        ON esc.school_id = sm.school_id
    LEFT JOIN {{ ref('int_bubble__school_volunteer_metrics') }} svm
        ON esc.school_id = svm.school_id
    LEFT JOIN volunteer_recruitment_target_per_school vrt
        ON esc.school_id = vrt.school_id
    LEFT JOIN e2_cpp_coc_counts_by_chapter cpp
        ON esc.chapter_id = cpp.chapter_id
    LEFT JOIN e2_cpp_coc_assigned_counts_by_chapter acpp
        ON esc.chapter_id = acpp.chapter_id
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
        COALESCE(bm.volunteers_signed_cpp_coc, 0) AS volunteers_signed_cpp_coc,
        COALESCE(bm.volunteers_signed_cpp_coc_assigned_to_class, 0) AS volunteers_signed_cpp_coc_assigned_to_class,
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
    FROM e2_chapters ch
    LEFT JOIN e2_bubble_metrics_by_chapter bm
        ON ch.chapter_id = bm.chapter_id
)

SELECT * FROM final
