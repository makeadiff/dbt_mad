{{ config(materialized='table') }}

-- School-level volunteer metrics (shared across prod_class_ops_data, prod_volunteer_recruitment)
-- Eliminates duplicate volunteers_per_school + volunteers_in_class CTEs
--
-- D1 fixes (see SRI_DASHBOARD_SPEC.md §5):
-- (a) `assigned` now filters is_active = true, not just is_removed = false, so prior-year
--     assignments that only got is_active flipped at rollover (not is_removed) stop counting.
-- (b) `recruited` now sources from int_bubble__school_volunteer_backfilled, which resolves null
--     school_id via the live class-assignment chain -- see that model's header for the bias
--     (undercounts schools with volunteers and no classes) and its expiry (a workaround for the
--     §6.8 platform defect, expected to be deleted once that's fixed upstream). That model also
--     enforces one row per (volunteer_id, academic_year) (2026-08-28) -- previously a volunteer
--     re-added to a second school without the first row being removed was counted at both
--     schools here; now backfilled itself resolves to the most recent.
-- (c) `recruited` counts COUNT(DISTINCT volunteer_id), not COUNT(*), so row duplication upstream
--     (int_bubble__school_volunteer's dedup) can never inflate this number, per D1(c)'s decision to
--     make the metric robust rather than chase a unique key.

WITH recruited AS (
    SELECT school_id, COUNT(DISTINCT volunteer_id) AS volunteers_recruited
    FROM {{ ref('int_bubble__school_volunteer_backfilled') }}
    WHERE school_id IS NOT NULL
    GROUP BY school_id
),

assigned AS (
    SELECT cs.school_id, COUNT(DISTINCT scsv.volunteer_id) AS volunteers_assigned_to_class
    FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
    INNER JOIN {{ ref('int_bubble__slot_class_section') }} scs
        ON scsv.slot_class_section_id = scs.slot_class_section_id
    INNER JOIN {{ ref('dim_class_section') }} cs
        ON scs.class_section_id = cs.class_section_id
    WHERE scsv.is_removed = false AND scs.is_removed = false AND scsv.is_active = true
    GROUP BY cs.school_id
)

SELECT
    COALESCE(r.school_id, a.school_id) AS school_id,
    COALESCE(r.volunteers_recruited, 0) AS volunteers_recruited,
    COALESCE(a.volunteers_assigned_to_class, 0) AS volunteers_assigned_to_class
FROM recruited r
FULL OUTER JOIN assigned a ON r.school_id = a.school_id
