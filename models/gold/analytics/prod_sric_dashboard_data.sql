{{
  config(
    materialized='table',
    description='SRIC dashboard data (E2 only) — chapter mapping enriched with Bubble child/volunteer counts and live recruitment/sourcing targets'
  )
}}

-- SRIC dashboard: E2 chapter mapping enriched with Bubble child/volunteer counts, CPP/COC
-- signing status, and live recruitment/sourcing targets.
-- Reaches into silver (int_google_sheet__chapter_mapping, int_bubble__partner,
-- int_bubble__slot_class_section, int_bubble__slot_class_section_volunteer,
-- int_bubble__school_volunteer_backfilled, int_pc_applicant_policy_status) rather than
-- marts-only, because no existing dim/fact carries co_id/cho_id/sourcing_campaign_code alongside
-- the chapter mapping, resolves per-volunteer CPP/COC status, carries partner.converted (needed
-- for D2/§6.6's active-chapter filter), exposes the assignment-level is_active flag that
-- fct_volunteer_slot_assignment drops (needed for D1(a)/D3's "currently allocated" correctness),
-- or backfills null school_id (a workaround with a known expiry -- see that model's header) --
-- same known deviation as prod_volunteer_recruitment.
-- volunteers_signed_cpp_coc / volunteers_signed_cpp_coc_assigned_to_class treat a volunteer as
-- "signed" only when both CPP and COC are accepted on their latest application (see
-- int_pc_applicant_policy_status's header for the unverified truthy-value assumption).
-- Grain: one row per E2 master mapping sheet row (chapter_id + cho_id)

-- D2 fix: chapter_status no longer comes from the sheet (see below) -- the sheet is a hand-entered
-- column validated only against 3 allowed values, and Sheet-Active (71) disagrees with
-- Bubble-converted (68) because some chapters are marked Active before MOU signature. The sheet is
-- kept here only for cho_id and sourcing_campaign_code, which no dim/fact carries.
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

-- D2 fix, revised per SRI_DASHBOARD_SPEC.md §6.6: chapter_status sources from Bubble, never the
-- sheet, but dim_chapter_current_status.is_currently_active ALONE isn't enough -- it agreed with
-- Session Ops's 68 for 68 chapters, but also called 4 more chapters "active" that have
-- dim_chapter_current_status.is_currently_active = true from an academic-year record while
-- int_bubble__partner.converted (MOU signed) is still false. Same failure mode D2 originally
-- described for the sheet (marked active before conversion), just one layer further down.
-- chapter_status is therefore is_currently_active AND converted; confirmed this combination
-- reconciles to exactly 68, matching Session Ops. Grain: one row per E2 chapter_id
e2_chapter_status AS (
    SELECT
        esc.chapter_id,
        COALESCE(dcs.is_currently_active, false) AND COALESCE(bp.converted, false) AS chapter_status
    FROM e2_schools_by_chapter esc
    LEFT JOIN {{ ref('dim_chapter_current_status') }} dcs
        ON esc.school_id = dcs.school_id
    LEFT JOIN {{ ref('int_bubble__partner') }} bp
        ON esc.school_id = bp.partner_id1
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

-- Formula (D10 fix): active_slot_class_section_count * 2. active_slot_class_section_count is
-- already COUNT(DISTINCT slot_class_section_id) -- a distinct count of (slot x section) pairs --
-- so multiplying by active_slot_count as well double-counted slots (2 slots x 5 sections/slot
-- produced 40 instead of the correct 20). See SRI_DASHBOARD_SPEC.md §3.1/D10.
volunteer_recruitment_target_per_school AS (
    SELECT
        school_id,
        active_slot_count,
        active_slot_class_section_count,
        (active_slot_class_section_count * 2)::integer AS volunteer_recruitment_target
    FROM active_slot_counts_per_school
),

-- D3 fix: recruited population now matches int_bubble__school_volunteer_metrics's D1(b)/D1(c)
-- fix (backfill null school_id via a live class assignment, count distinct volunteer_id) instead
-- of joining fct_school_volunteer.school_id directly. That direct join undercounted recruited
-- volunteers (school_id is NULL on ~70% of raw rows -- a Bubble/Airbyte data gap, not a broken
-- join; see SRI_DASHBOARD_SPEC.md §6.3), which is why C1's "signed" numerator (4) came out smaller
-- than the "assigned-and-signed" figure (62) it should have contained -- two different populations
-- were being compared as if one were a subset of the other. Per §4 C1, recruited is the one
-- denominator; this keeps it consistent everywhere it's used. Backfill logic lives in
-- int_bubble__school_volunteer_backfilled -- see that model's header for the bias and its expiry
-- (a workaround for §6.8, expected to be deleted once fixed upstream).
-- Grain: one row per E2 chapter_id + recruited volunteer_id
e2_recruited_volunteers AS (
    SELECT
        esc.chapter_id,
        rv.volunteer_id
    FROM e2_schools_by_chapter esc
    INNER JOIN {{ ref('int_bubble__school_volunteer_backfilled') }} rv
        ON esc.school_id = rv.school_id
),

-- Allocated = currently allocated (§2): filters scsv.is_active = true, matching D1(a), rather than
-- going through fct_volunteer_slot_assignment, which doesn't expose the assignment-level is_active
-- flag (only the class-section-level one) and would carry the same rollover-asymmetry bug as D1(a).
-- Grain: one row per E2 chapter_id + volunteer_id currently assigned to a class section
e2_assigned_volunteers AS (
    SELECT DISTINCT
        esc.chapter_id,
        scsv.volunteer_id
    FROM e2_schools_by_chapter esc
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON cs.school_id = esc.school_id
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON scs.class_section_id = cs.class_section_id
    INNER JOIN {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    WHERE scsv.is_removed = false AND scs.is_removed = false AND scsv.is_active = true
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
-- §3.4: volunteers_currently_allocated, the population volunteers_unallocated subtracts from
-- volunteers_recruited. Reuses e2_assigned_volunteers (D1(a) semantics) rather than re-deriving.
e2_allocated_counts_by_chapter AS (
    SELECT
        chapter_id,
        COUNT(DISTINCT volunteer_id) AS volunteers_currently_allocated
    FROM e2_assigned_volunteers
    GROUP BY chapter_id
),

-- Grain: one row per E2 chapter_id
-- B3, Row 1 (§8, SIGNED 2026-08-26): two coverage metrics, not one fill rate --
-- class_fill_rate_pct is dropped ("three overlapping answers to one question is how the original
-- dashboard went wrong"). Both derive from volunteers_on_class in fct_class_section_coverage:
-- started = sections with >= 1 volunteer (classes can run at all); fully_staffed = sections with
-- the full §3.1 target of 2. The 0/1/2/3+ bucket distribution stays available as detail.
-- total_active_sections requires BOTH dim_class_section.is_active AND a live slot_class_section
-- link (matching active_slot_counts_per_school's join) -- a class section can be marked active in
-- Bubble with zero slot linkage (school 427: 22 active sections, 0 slot-mapped, per §6.4), and a
-- section nobody has scheduled a slot for cannot run a session, so it isn't a real denominator.
-- Confirmed 2026-08-26: only 9 E2 chapters have any active slot-linked sections at all -- ground
-- truth, not a defect (§6.4/§6.5). Before this fix, chapters with sections-but-no-slots (e.g.
-- 178, 181) showed 0% here instead of NULL, which would rank them ABOVE chapters with genuinely
-- poor coverage in a worst-performers list.
e2_class_coverage_by_chapter AS (
    SELECT
        esc.chapter_id,
        COUNT(DISTINCT cs.class_section_id) AS total_active_sections,
        COUNT(DISTINCT cs.class_section_id) FILTER (WHERE fcc.volunteers_on_class >= 1) AS started_sections,
        COUNT(DISTINCT cs.class_section_id) FILTER (WHERE fcc.volunteers_on_class >= 2) AS fully_staffed_sections
    FROM e2_schools_by_chapter esc
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON esc.school_id = cs.school_id
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON scs.class_section_id = cs.class_section_id
    LEFT JOIN {{ ref('fct_class_section_coverage') }} fcc
        ON fcc.class_section_id = cs.class_section_id
    WHERE cs.is_active = true
      AND scs.is_removed = false
      AND scs.is_active = true
    GROUP BY esc.chapter_id
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
        COALESCE(SUM(acpp.volunteers_signed_cpp_coc_assigned_to_class), 0) AS volunteers_signed_cpp_coc_assigned_to_class,
        COALESCE(SUM(alloc.volunteers_currently_allocated), 0) AS volunteers_currently_allocated,
        COALESCE(SUM(cov.total_active_sections), 0) AS total_active_sections,
        COALESCE(SUM(cov.started_sections), 0) AS started_sections,
        COALESCE(SUM(cov.fully_staffed_sections), 0) AS fully_staffed_sections
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
    LEFT JOIN e2_allocated_counts_by_chapter alloc
        ON esc.chapter_id = alloc.chapter_id
    LEFT JOIN e2_class_coverage_by_chapter cov
        ON esc.chapter_id = cov.chapter_id
    GROUP BY esc.chapter_id
),

-- §12a: output columns are named for the dashboard, not the model internals -- snake_case here is
-- the BI label (Dialogue auto-titles it), so this is the one place in the model where naming
-- follows the rename map, not the upstream column names. Aliased at this gold layer only;
-- int_bubble__school_volunteer_metrics keeps its original names since prod_class_ops_data and
-- prod_volunteer_recruitment also depend on it.
final AS (
    SELECT
        ch.city_name AS city,
        ch.state,
        ch.co_id,
        ch.co_name AS community_organiser,
        ch.chapter_id,
        ch.chapter_name AS chapter,
        ch.worknode_id,
        ch.cho_id,
        ch.cho_name AS chapter_organiser,
        ch.engine,
        COALESCE(cs.chapter_status, false) AS chapter_status,
        ch.sourcing_campaign_code AS sourcing_campaign,
        COALESCE(bm.active_child_count, 0) AS total_children,
        COALESCE(bm.active_slot_count, 0) AS active_slots,
        COALESCE(bm.active_slot_class_section_count, 0) AS active_class_sections,
        -- D11 fix: distinguishes a genuine zero target (no active class sections) from a broken
        -- join that would otherwise render identically as 0. See §3.5.
        COALESCE(bm.active_slot_class_section_count, 0) > 0 AS classes_set_up,
        COALESCE(bm.volunteer_count, 0) AS volunteers_allocated_to_school,
        COALESCE(bm.volunteers_assigned_to_class, 0) AS volunteers_assigned_to_class,
        COALESCE(bm.volunteers_signed_cpp_coc, 0) AS volunteers_compliant,
        COALESCE(bm.volunteers_signed_cpp_coc_assigned_to_class, 0) AS volunteers_compliant_assigned_to_class,
        COALESCE(bm.volunteer_recruitment_target, 0) AS volunteers_required,
        COALESCE(bm.volunteer_recruitment_target, 0) * 3 AS leads_required,
        GREATEST(
            0,
            COALESCE(bm.volunteer_recruitment_target, 0) - COALESCE(bm.volunteer_count, 0)
        ) AS volunteers_still_to_recruit,
        GREATEST(
            0,
            COALESCE(bm.volunteer_recruitment_target, 0) - COALESCE(bm.volunteer_count, 0)
        ) * 3 AS leads_still_to_source,
        -- §3.4 / Row 2a: recruited but not currently placed -- its own row, not silently absorbed
        -- into the recruitment target.
        GREATEST(
            0,
            COALESCE(bm.volunteer_count, 0) - COALESCE(bm.volunteers_currently_allocated, 0)
        ) AS volunteers_unallocated,
        -- Row 5: % of recruitment target met. Null (not 0) where there's no target to measure
        -- against, per §3.5's zero-vs-missing rule -- suppressed in Dialogue wherever
        -- classes_set_up is false, same as the raw target.
        -- NOTE: unlike §4's population ratios (numerator always a subset of denominator, capped
        -- at 100% by construction), this is an achievement-vs-target ratio and CAN legitimately
        -- exceed 100% -- a chapter can be over-resourced relative to its currently active
        -- sections (confirmed: chapter 443, 10 mapped vs a target of 2 from a single active
        -- section). Not a defect; do not clamp it.
        CASE
            WHEN COALESCE(bm.volunteer_recruitment_target, 0) > 0
            THEN ROUND(100.0 * COALESCE(bm.volunteer_count, 0) / bm.volunteer_recruitment_target, 1)
        END AS recruitment_target_met_pct,
        -- Row 1, SIGNED 2026-08-26: two coverage metrics, not one fill rate (class_fill_rate_pct
        -- dropped). Both null where the chapter has no active sections at all.
        -- Raw counts exposed alongside the per-chapter %s (active_class_sections is already the
        -- denominator) so the dashboard can roll up SUM(started)/SUM(total) at any filter level --
        -- averaging the per-chapter percentages would weight a 2-section chapter the same as a
        -- 200-section one and give the wrong national number.
        COALESCE(bm.started_sections, 0) AS class_sections_started,
        COALESCE(bm.fully_staffed_sections, 0) AS class_sections_fully_staffed,
        CASE
            WHEN COALESCE(bm.total_active_sections, 0) > 0
            THEN ROUND(100.0 * COALESCE(bm.started_sections, 0) / bm.total_active_sections, 1)
        END AS class_sections_started_pct,
        CASE
            WHEN COALESCE(bm.total_active_sections, 0) > 0
            THEN ROUND(100.0 * COALESCE(bm.fully_staffed_sections, 0) / bm.total_active_sections, 1)
        END AS class_sections_fully_staffed_pct
    FROM e2_chapters ch
    LEFT JOIN e2_bubble_metrics_by_chapter bm
        ON ch.chapter_id = bm.chapter_id
    LEFT JOIN e2_chapter_status cs
        ON ch.chapter_id = cs.chapter_id
)

SELECT * FROM final
