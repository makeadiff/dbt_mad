-- D1(c) acceptance criterion: dedup on school_volunteer_id must not silently drop rows.
-- school_volunteer_id is unique going in (confirmed 2026-08-26), so the deduplicated model
-- should have exactly one row per distinct school_volunteer_id on the raw input -- not fewer.
-- Fails (returns rows) if that count ever diverges, surfacing a future collision instead of
-- silently dropping rows the way the original D1(c) defect did.
with expected as (
    select count(distinct school_volunteer_id) as expected_rows
    from {{ ref('stg_bubble__school_volunteer') }}
),
actual as (
    select count(*) as actual_rows
    from {{ ref('int_bubble__school_volunteer') }}
)
select expected.expected_rows, actual.actual_rows
from expected, actual
where expected.expected_rows <> actual.actual_rows
