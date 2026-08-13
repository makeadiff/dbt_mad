# E2 Chapter Dashboard — Build Notes (temp working doc)

Working context for rebuilding the old `fct_dashboard_summary` model (from
`mad_dbt__old_models/prod/analytics/fct_dashboard_summary.sql`) on the current bronze/silver/marts/gold
structure. This is a scratch doc to track decisions and pending items while the build is in progress —
not permanent project documentation.

## What this is

An E2-only (Bubble + DOTS sourced) chapter x academic-year ops dashboard, covering session delivery,
coverage gaps, and volunteer/child consistency. Everything built for it is tagged `fct_e2_*` /
`prod_e2_*` because an E1 dashboard is planned later on a different data source — see "Future work"
below.

## Models built this pass

**New silver:**
- `int_bubble__academic_year`, `int_bubble__class`, `int_bubble__school_holiday`

**Modified silver** (additive columns only):
- `int_bubble__slot` — added resolved `school_academic_year_id`
- `int_bubble__school_class` — added resolved `school_academic_year_id`
- `int_bubble__school_volunteer` — added `is_active` (was being dropped)

**New marts/core dimensions:**
- `dim_chapter_mapping` — one row per chapter, Google Sheet ops-mapping + Bubble partner fallback for
  name; also carries `worknode_id`
- `dim_school_academic_year_status` — one row per (school_id, academic_year); resolves the *latest*
  `school_academic_year` row per business key and exposes `is_ay_active`

**New marts/core facts:**
- `fct_e2_volunteer_allocation_history`, `fct_e2_child_allocation_history`
- `fct_e2_volunteer_attendance_by_slot_date`, `fct_e2_child_attendance_by_slot_date`
- `fct_e2_cancellations`
- `fct_e2_cancellation_reasons` — cancelled sessions broken down by cancellation reason (Bubble's
  `holiday_reason`, e.g. "Cancelled from school's end", "MAD event (eg: YEC, etc)"), grain
  `(chapter_id, academic_year, cancellation_reason)`. Sibling of `fct_e2_cancellations` (same
  planned-vs-holiday projection), not built on top of it, since `fct_e2_cancellations` only exposes a
  distinct-reasons string per section, not per-reason counts.
- `fct_e2_volunteer_consistency`, `fct_e2_child_consistency`
- `fct_e2_school_coverage` — coverage/footprint metrics: `total_sections`, `total_slots`,
  `total_classes`, `total_volunteers_assigned`, `is_chapter_active`, mentor-coverage gap counts
- `fct_e2_sessions_summary`
- `fct_e2_volunteer_recruitment` — currently just `total_volunteers_in_school` for 2026-2027

**New gold:**
- `prod_e2_dashboard_summary` — the dashboard fact itself
- `prod_e2_volunteer_consistency` — row-level volunteer consistency, for a standalone volunteer
  consistency dashboard. Built on `fct_e2_volunteer_consistency` + `dim_chapter_mapping`.
- `prod_e2_child_consistency` — same pattern for children, built on `fct_e2_child_consistency` +
  `dim_chapter_mapping`.

## Key decisions made along the way

- **Partner join key**: use `int_bubble__partner` (bubble-side, `partner_id1`) everywhere a school/chapter
  join is needed in the E2 chain, not the CRM-side `int_crm__partners` — except inside
  `int_google_sheet__chapter_mapping`'s own `chapter_validation` check, which legitimately uses CRM
  partners (that's how the legacy model did it too).
- **`fct_e2_volunteer_allocation_history`** was rebuilt independent of
  `gold/external_apps/prod_volunteer_allocation_history.sql` (that file is a separate, child-expanded
  view for one app's needs — left untouched).
- **Chapter active-status-per-year** (`dim_school_academic_year_status`): a school can have more than one
  `school_academic_year_id` PK mapped to the same (school_id, academic_year_id) business key over time.
  We take the *latest* one (by modified_date) and read its `is_active`/`is_removed` as that school's
  status for that year. Deliberately not folded into `int_bubble__school_academic_year` itself, since
  that model is a shared FK-resolution table other models join to by PK — collapsing its grain would
  silently drop rows those joins depend on.
- **Active-year vs archived-year counting pattern** (`total_slots`, `total_classes`,
  `total_volunteers_assigned` in `fct_e2_school_coverage`): if `dim_school_academic_year_status` says a
  school's year is active, count the live rows (`is_active=true, is_removed=false`); if that year has
  since been archived, Bubble keeps the rows around with `is_active` flipped to false rather than
  removing them, so count `is_active=false, is_removed=false` instead. Same branching logic reused
  across all three metrics.
- **`total_volunteers_in_school`** (2026-2027 only): `school_volunteer` was the 2025-2026 recruitment
  intake bucket (COs added volunteers here, then manually assigned them into slots). That workflow was
  retired going into 2026-2027, but the table still reflects the live/current set of volunteers
  considered "in" a school, so `is_active=true, is_removed=false` on that table is used directly, no
  branching needed since we're only asking for the current year.
- **`total_volunteers_assigned`** walks `slot` (via its `school_academic_year_id`) →
  `slot_class_section` → `slot_class_section_volunteer`, not through
  `fct_e2_volunteer_allocation_history` (which also requires the parent `slot_class_section` to be
  non-removed — stricter than what was asked for here).
- **Dashboard-facing layering rule**: `gold/analytics` models must build on `marts/core` dims/facts
  only, never on another `gold/analytics` model (`gold/external_apps` is for API-facing outputs;
  `gold/analytics` is for dashboards). `fct_e2_volunteer_consistency`/`fct_e2_child_consistency`
  briefly got moved into `gold/analytics` and renamed `prod_e2_*` to serve the new consistency
  dashboards — that was wrong, since it made `prod_e2_dashboard_summary` (itself gold/analytics)
  depend on another gold/analytics model. Reverted: the two facts stay in `marts/core/facts` as
  `fct_e2_*` (they're genuinely shared — consumed by both the rollup and the standalone dashboards),
  and two new sibling gold/analytics models (`prod_e2_volunteer_consistency`,
  `prod_e2_child_consistency`) were added on top instead, each independently built on the marts fact
  + `dim_chapter_mapping`. `prod_e2_dashboard_summary` was never touched by this — it always read the
  marts facts directly.
- **Cancellation reason breakdown added to `prod_e2_dashboard_status_mix`** (2026-08-13), as a new
  `'Cancellation Reason'` metric_group for a pie chart. Source is `fct_e2_cancellation_reasons`
  (new), which groups the same section-level planned-vs-holiday projection used by
  `fct_e2_cancellations` by Bubble's `holiday_reason` field instead of collapsing it to a string.
  Category values are passed through as Bubble's raw text (not remapped into fixed buckets like
  "School's side"/"MAD"/"Event"), so new dropdown values in Bubble show up automatically without a
  model change. Caveat: as of this date `holiday_reason` is barely populated — only 2 rows exist in
  `school_holiday` warehouse-wide (one of them `is_removed=true`) — so expect this breakdown to look
  near-empty in the dashboard until more chapters log cancellations this way.
- **`int_bubble__school_holiday` dedup bug found + fixed** (2026-08-13). It was deduplicating on
  `school_holiday_id`, following the same pattern every other `int_bubble__*` model uses for its own
  entity id -- but unlike those, `school_holiday_id` isn't a reliable global unique key here: the only
  2 real rows in the whole warehouse are for two different schools and both are stamped
  `school_holiday_id=1`, so the dedup silently kept only one and dropped the other as a "duplicate".
  Fixed to dedupe on Bubble's raw `_id` instead. Confirmed fix: `int_bubble__school_holiday` now
  returns 2 rows (was 1). No observed effect on `fct_e2_cancellations`' existing totals today, since
  the previously-dropped row has `is_removed=true` anyway -- but it was real data loss that would have
  compounded as more holiday rows get added, and it silently affected `fct_e2_cancellations` (already
  live in prod) too, not just the new reason breakdown. Confirmed separately that the one surviving,
  non-removed row (school 509, holiday dated Friday 2026-07-03) still produces zero cancellations
  because that school's sections only meet on Saturday -- `fct_e2_cancellations`/
  `fct_e2_cancellation_reasons` only detect cancellations that land on a school's actual recurring
  weekly class day; a holiday falling on a day that never has a class was never going to be caught by
  this design, dedup bug or not.
- **`dim_chapter_mapping.chapter_name`** now sources from Bubble (`int_bubble__partner.partner_name`)
  instead of the ops sheet — the sheet is human-maintained and had real mismatches against Bubble for a
  meaningful subset of chapters (e.g. two chapters whose names looked swapped between the two sources).
  `city_name`/`co_name`/`cho_name`/`engine`/`chapter_status` still come from the sheet since Bubble has
  no equivalent for those.
- **The dashboard's row set (which chapter+academic_year combos exist at all) no longer comes from
  `dim_chapter_mapping`.** It used to cross-join `dim_chapter_mapping` filtered to
  `chapter_status='Active' AND engine='E2'` with every academic year — but that's *today's* ops-sheet
  status, and it was silently excluding schools with real historical data: for 2025-2026 alone, 35
  schools marked "Dropped out" (but still with real 2025-2026 session data in Bubble) and 15 schools
  never added to the sheet at all. `fct_e2_sessions_summary`'s `chapter_academic_years` CTE now drives
  off `dim_school_academic_year_status` instead (pure Bubble: one row per school × academic year that
  actually exists in `school_academic_year`) — `dim_chapter_mapping` is still left-joined, but only to
  enrich with city/CO/CHO/chapter_status, never to decide which rows appear. Effect: dashboard row
  count went from 142 to 187 (119 for 2025-2026, up from 71; 68 for 2026-2027, up from 71). No explicit
  engine filter is applied any more either — every school in `school_academic_year` is assumed to be E2
  by construction (Bubble/DOTS has no E1 presence), flagged in `fct_e2_sessions_summary`'s header
  comment in case that assumption ever needs revisiting.
- **`chapter_status` now comes from `dim_school_academic_year_status.is_ay_active`** (`'Active'` /
  `'Dropped out'`) across all three E2 dashboards, not from `dim_chapter_mapping`'s ops-sheet status.
  Traced why the sheet-based version had nulls: of 561 Bubble partners, 454 simply have no matching
  `chapter_id` row in the ops sheet at all (confirmed zero cases of "matched to a sheet row but the
  status cell was blank" — it's purely a missing-row problem). The sheet's status is also only a
  single current-state value, not per academic year, so it couldn't distinguish "active in 2025-2026"
  from "active in 2026-2027" anyway. The new derivation is per-`(chapter_id, academic_year)`, sourced
  entirely from Bubble, and has zero nulls across all 187 dashboard rows.
- **`chapter_status` moved again, this time to chapter-level.** The per-year 'Active'/'Dropped out'
  version above was itself wrong: a chapter's *old* year showing inactive is usually just normal
  rollover once a newer year exists (of 101 chapters inactive for 2025-2026, 62 are active again in
  2026-2027) — "dropped out" isn't something a single year's flags can prove. New model
  `dim_chapter_current_status` (grain: one row per chapter) finds each chapter's single latest
  academic-year record (by `academic_year_id`, so it generalizes to however many years exist) and
  reports whether *that one* is active, as `'Active'`/`'Inactive'`. This value is now identical across
  every row for the same chapter regardless of which academic_year that row is for. Wired into all
  three E2 dashboards. `dim_school_academic_year_status` (per-year) is unchanged and still used for the
  `is_chapter_active`/`total_slots`/`total_classes`/`total_volunteers_assigned` branching logic in
  `fct_e2_school_coverage`, which genuinely needs a per-year answer.

## Pending / open items

0. **Data quality: `int_bubble__children.is_active = false` but that child's `int_bubble__child_class`
   row is still `is_active = true, is_removed = false`** (found 2026-08-13, while investigating the
   141-child `total_children_in_system` vs mentor-coverage gap for 2026-2027, see the decision entry
   above). **2,061 distinct children warehouse-wide** are affected (1,551 tagged `academic_year =
   '2025-2026'` on the `child_class` row, 510 with a null `academic_year`). This was inflating
   `fct_e2_school_coverage.total_children_in_system`, which only filtered on `child_class.is_removed =
   false` — it didn't check the child's own `is_active` flag, so exited/inactive children with a
   stale-active `child_class` row were still counted as "in system."
   - Concretely traced for chapter 220 (2026-2027): of the 62 children counted in
     `total_children_in_system` but missing a `child_class_section` assignment (the biggest single
     contributor to the 177-child side of the gap), **all 62** were `is_active = false` on the child
     record — none were a real "enrolled, pending section assignment" case. Across all 177, 111 (63%)
     were inactive/removed children; only 66 were genuinely active kids waiting on assignment.
   - **Fixed in `fct_e2_school_coverage`** (2026-08-13): `children_in_system` now joins
     `int_bubble__children` and requires `is_active = true, is_removed = false` there. Deliberately did
     **not** also filter `child_class.is_active = true` itself -- tested it and it collapses 2025-2026
     from 3,327 to 244 children, reproducing the exact undercounting the model's older comment already
     warns about (child_class's `is_active` doesn't reliably flip false on archival). The same
     `is_active=true` filter (via `int_bubble__children`) was then also added to
     `total_children_with_mentor`/`children_without_mentor` in `section_level_metrics`, since without it
     the mentor-side totals started *exceeding* `total_children_in_system` once only the system-total
     side had the filter (a -47 gap for 2025-2026).
   - Result: `total_children_in_system` vs `(total_children_with_mentor + children_without_mentor)` gap
     shrank from 141→30 (2026-2027) and flipped from -47→+31 (2025-2026, i.e. no longer negative). The
     remaining ~30/year is the pre-existing, still-expected structural gap (child enrolled via
     `child_class` for one year but section-assigned under a different year, or vice versa -- see the
     rollover-lag example traced earlier for child 443 at chapter 509) -- not the exited-children issue,
     which is now resolved on the model side. **The underlying Bubble data (stale-active `child_class`
     rows for exited children) still needs cleanup** -- the model fix filters it out of these dashboard
     metrics, but doesn't fix the source data. Cleanup query for ops is in the conversation this was
     found in.

0b. **Two more dedup-key bugs found and fixed (2026-08-14), same class as the `school_holiday_id`
    bug documented in the decisions above.** Traced by hand-checking a specific child (4724) who
    appeared in the "active enrollment, no section assignment" gap (item 0's 66-child group) despite
    the user finding a real `child_class_section` record for them in bronze. Root cause: both
    `int_bubble__child_class` and `int_bubble__child_class_section` were deduplicating on Bubble's own
    entity-id field (`child_class_id`, `child_class_section_id`), which -- like `school_holiday_id` --
    is **not actually a reliable global unique key**. Confirmed with `child_class_section_id=7211`:
    two genuinely different rows (child 4724 and child 4725, same `modified_date`, both assigned to
    class_section 1564) shared that id; the dedup kept only 4725's row, making 4724 look like they had
    no section assignment at all.
    - **`int_bubble__child_class_section`**: 41 colliding id groups, 87 raw rows involved (~46 rows
      were being silently dropped). Fixed to dedupe on raw `_id` instead.
    - **`int_bubble__child_class`**: 46 colliding id groups, 94 raw rows involved. Same fix applied.
    - **Impact, once both were fixed and rebuilt**: the "active enrollment, no section assignment"
      side of the 2026-2027 gap (item 0's 66 children) collapsed to **1** -- 65 of the 66 already had
      a real section assignment, just hidden by this bug. `total_children_with_mentor` for 2026-2027
      jumped from 5 to 58 for the same reason. Combined with the `is_active` fix (item 0 above),
      `total_children_in_system` vs `(total_children_with_mentor + children_without_mentor)` now
      reconciles almost exactly: **gap = 0 for 2025-2026, gap = 1 for 2026-2027** (down from the
      original 141 / -47).
    - **Also found, NOT fixed yet** (out of scope for the E2 dashboards -- feeds a different consumer,
      `gold/external_apps/prod_child_removal_log_migration.sql`): `int_bubble__child_removal_log` has
      the exact same bug on `child_removal_log_id` -- **173 colliding groups, 1,825 raw rows
      involved**, much bigger than the two fixed above. Needs its own look before fixing, since it
      feeds a different downstream model this session didn't touch.
    - **Not yet fully audited**: checked `academic_year_id`, `child_id`, `class_id`,
      `class_section_id`, `class_section_subject_id` clean (0 collisions). Did not finish checking
      `partner_id1`, `school_academic_year_id`, `school_class_id`, `school_volunteer_id`, `slot_id`,
      `slot_class_section_id`, `slot_class_section_volunteer_id` (audit was interrupted by a timeout)
      -- worth a follow-up pass given two confirmed hits out of the models checked so far.

1. **`total_volunteers_in_school` for 2025-2026 — parked, needs a larger discussion.** Not being
   guessed at. (For reference when that discussion happens: the filter originally proposed,
   `is_active=false AND is_removed=false`, matches zero rows in `school_volunteer`; the combos that
   actually exist are `is_removed=true` — 1,392 rows, mostly tagged `academic_year='2025-2026'` — or
   `is_active=true, is_removed=false` — 4,121 rows, mostly `academic_year` null.)

2. **RESOLVED by the row-set change above.** Previously, chapters with no `school_academic_year` row
   for a given year still appeared as a blank row (null `is_chapter_active`/`total_slots`/etc.), which
   was confusing. Now that the row set itself comes from `dim_school_academic_year_status`, a
   chapter/year with no `school_academic_year` row simply has no row in the dashboard for that year —
   no more silent nulls to explain. (Historical note, no longer current now that the row set changed:

   **5 chapters missing 2025-2026 status:**
   | chapter_id | chapter_name | city | CO |
   |---|---|---|---|
   | 175 | Anglo Sanskrit Victoria Jubilee Sr Sec School | Delhi | Tanya Sharma |
   | 455 | Shree G.C. Primary School | Ahmedabad | Saniya Mahajan |
   | 594 | SANTHI ENGLISH MEDIUM SCHOOL | Guntur | Prem Naren |
   | 600 | St. Crispins Home | Pune | Anavi Bodhani |
   | 604 | Govt. Hr. Sec. School Janakganj 2 | Gwalior | Ashish sharma |

   **12 chapters missing 2026-2027 status:**
   | chapter_id | chapter_name | city | CO |
   |---|---|---|---|
   | 175 | Anglo Sanskrit Victoria Jubilee Sr Sec School | Delhi | Tanya Sharma |
   | 305 | Global Kids English Medium School | Vijayawada | Harshitha K |
   | 357 | RKD NSS HSS SASTHAMANGALAM | Trivandrum | Rasha Henna |
   | 431 | Nava Vadaj Municipal Gujarati School No. 9 | Ahmedabad | Saniya Mahajan |
   | 455 | Shree G.C. Primary School | Ahmedabad | Saniya Mahajan |
   | 532 | SSNKM Sharda Mandir | Cochin | Aarya gopan |
   | 567 | Government Higher Secondary School, Maduravoyal | Chennai | Sree Krithigha R |
   | 568 | Govt ADW High School, Thirumangalam | Chennai | Sree Krithigha R |
   | 587 | Manav Sadhna | Ahmedabad | Saniya Mahajan |
   | 594 | SANTHI ENGLISH MEDIUM SCHOOL | Guntur | Prem Naren |
   | 600 | St. Crispins Home | Pune | Anavi Bodhani |
   | 604 | Govt. Hr. Sec. School Janakganj 2 | Gwalior | Ashish sharma |

   Note chapter 175, 455, 594, 600, 604 appear in *both* lists — missing a `school_academic_year` row
   for either year entirely. That's still true after the row-set change — these 5 schools have no
   Bubble academic-year record for either year, so they still have no dashboard row at all. Status:
   waiting on confirmation of whether this is an onboarding lag or a real data gap to flag to ops.)

3. **`dim_chapter_mapping` is current-state only, not historized** — acknowledged, not a concern for now.
   The `worknode_id` mapping it depends on is itself a this-year-only concept, so there's no
   "historical" version of it to reconcile against anyway.

4. **No time-series/trend fact** — deferred until the dashboard is functionally complete and accuracy
   has been raised on what's already built. Everything so far is a per-academic-year snapshot only.

5. **E1 dashboard** — deferred until E2 is complete. Will need its own `fct_e1_*` chain once E1's actual
   source system is confirmed.

6. **Confirm the expanded row set is actually wanted.** The dashboard now includes every school with
   real Bubble academic-year data, including 40 rows currently marked "Dropped out" on the ops sheet
   and 15 rows not on the sheet at all (both show up with null `city_name`/`co_name`/`chapter_status`
   since Bubble has no equivalent for those fields). This is a deliberate change from "only today's
   Active+E2 chapters" to "every school with real historical data" — worth a sanity check with whoever
   consumes this dashboard before it's treated as final, since totals (chapter counts, sums) will look
   noticeably different from before.

## Prod deployment

Everything through `dim_chapter_current_status` / `prod_e2_dashboard_status_mix` (added in a later
session than the original list below) is confirmed already live in `prod_gold_marts` /
`prod_gold_analytics` as of 2026-08-13 — checked directly against `information_schema.tables`. Original
scoped command, for reference (assumes everything these reference that wasn't touched this session —
`stg_bubble__partner`, `int_bubble__partner`, `int_dots__*`, `int_crm__partners`, `int_pc_user_data`,
`int_google_sheet__chapter_mapping`, etc. — already exists in prod from regular runs):

```
dbt run --target prod --select \
  int_bubble__academic_year int_bubble__class int_bubble__school_holiday \
  int_bubble__slot int_bubble__school_class int_bubble__school_volunteer \
  dim_chapter_mapping dim_school_academic_year_status dim_chapter_current_status \
  fct_e2_volunteer_allocation_history fct_e2_child_allocation_history \
  fct_e2_volunteer_attendance_by_slot_date fct_e2_child_attendance_by_slot_date \
  fct_e2_cancellations fct_e2_cancellation_reasons fct_e2_volunteer_consistency fct_e2_child_consistency \
  fct_e2_school_coverage fct_e2_sessions_summary fct_e2_volunteer_recruitment \
  prod_e2_dashboard_summary prod_e2_dashboard_status_mix prod_e2_volunteer_consistency prod_e2_child_consistency
```

**Incremental command for the 2026-08-13 cancellation-reason-breakdown change specifically.**
`fct_e2_cancellation_reasons` is new. `int_bubble__school_holiday` is an existing prod model whose
dedup key was fixed this pass (see decision above) — it and everything downstream of it that's already
live in prod needs a rebuild too, not just the new pieces, since the fix changes its row set:

```
dbt run --target prod --select \
  int_bubble__school_holiday fct_e2_cancellations fct_e2_cancellation_reasons \
  fct_e2_sessions_summary prod_e2_dashboard_summary prod_e2_dashboard_status_mix
```

**Incremental command for the 2026-08-13 `total_children_in_system`/mentor-coverage `is_active` fix**
(pending item 0 above). Only `fct_e2_school_coverage` changed, but both dashboards read it:

```
dbt run --target prod --select \
  fct_e2_school_coverage prod_e2_dashboard_summary prod_e2_dashboard_status_mix
```

**Incremental command for the 2026-08-14 `child_class`/`child_class_section` dedup-key fixes**
(pending item 0b above). Both are existing prod silver models whose dedup key changed — everything
downstream that's already live in prod needs a rebuild too, not just re-running the two silver models
alone (their row sets changed, so everything reading them needs fresh data):

```
dbt run --target prod --select \
  int_bubble__child_class int_bubble__child_class_section \
  fct_e2_school_coverage prod_e2_dashboard_summary prod_e2_dashboard_status_mix
```

Re-run this doc's "Prod deployment" section (updating the model list) after every further change so the
command stays accurate.
