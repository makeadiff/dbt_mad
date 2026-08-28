-- §6.10, SIGNED 2026-08-26: Coverage is five STATES, not a funnel -- Allocated to School,
-- Compliant, Onboarded, Allocated to Class, Ready to Mentor Children come from independent
-- systems that do not nest by stage_order. Confirmed: is_compliant reads PC applicant-policy
-- records, is_allocated_to_class reads Bubble slot assignments -- a volunteer can be compliant
-- without a live class assignment, so Compliant > Allocated to Class is a real, correct state,
-- not a bug. Per §6.8, is_allocated_to_school is also undercounted (backfill drops null-school
-- rows with no class assignment), so Allocated to Class > Allocated to School is possible too.
-- Do NOT assert Compliant <= Allocated to Class, or any other stage_order-based ordering -- that
-- would be forcing a sequence onto data that was never sequential.
--
-- STAGE ORDER, REVISED 2026-08-27: display order now follows the real process (compliance and
-- induction gate class allocation), not build order -- 1 Allocated to School, 2 Compliant,
-- 3 Onboarded, 4 Allocated to Class, 5 Ready to Mentor Children. This makes Allocated to Class
-- (48) sitting above Compliant (6) VISIBLE as a process violation in the chart -- intentional,
-- not something to fix by reordering back. This test's stage_order filters below are updated to
-- match; the underlying invariants are unchanged (they follow fct_volunteer_pipeline's boolean
-- construction, not display order).
--
-- Only two invariants hold by construction (from fct_volunteer_pipeline's own boolean logic, not
-- from any assumed ordering or the display order above):
--   Onboarded <= Compliant              (is_onboarded = is_compliant AND inducted)
--   Ready to Mentor Children <= Allocated to Class, <= Compliant, <= Onboarded
--     (is_ready_to_mentor = is_allocated_to_class AND is_compliant AND inducted)
-- Uses stage_order, not stage_name, to identify stages -- stage_name carries an order prefix
-- ("2 · Compliant") for Dalgo's alphabetical X-axis sort (§14), so a literal stage_name match
-- here would silently match nothing and make this test vacuously pass.
with pivoted as (
    select
        chapter_id,
        volunteer_source,
        max(volunteers) filter (where stage_order = 2) as compliant,
        max(volunteers) filter (where stage_order = 3) as onboarded,
        max(volunteers) filter (where stage_order = 4) as allocated_to_class,
        max(volunteers) filter (where stage_order = 5) as ready_to_mentor
    from {{ ref('prod_sric_funnel') }}
    where funnel_block = 'coverage'
    group by chapter_id, volunteer_source
)
select *
from pivoted
where onboarded > compliant
   or ready_to_mentor > allocated_to_class
   or ready_to_mentor > compliant
   or ready_to_mentor > onboarded
