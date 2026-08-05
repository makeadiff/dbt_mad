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

## Pending / open items

1. **`total_volunteers_in_school` for 2025-2026 — parked, needs a larger discussion.** Not being
   guessed at. (For reference when that discussion happens: the filter originally proposed,
   `is_active=false AND is_removed=false`, matches zero rows in `school_volunteer`; the combos that
   actually exist are `is_removed=true` — 1,392 rows, mostly tagged `academic_year='2025-2026'` — or
   `is_active=true, is_removed=false` — 4,121 rows, mostly `academic_year` null.)

2. **Chapters with no `school_academic_year` row at all for a given year** — these come back null for
   `is_chapter_active`/`total_slots`/`total_classes`/`total_volunteers_assigned`, and their underlying
   slot/class/volunteer rows are silently excluded from those sums (not just cosmetically null).
   As of this build:

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
   for either year entirely. Status: waiting on confirmation of whether this is an onboarding lag or a
   real data gap to flag to ops.

3. **`dim_chapter_mapping` is current-state only, not historized** — acknowledged, not a concern for now.
   The `worknode_id` mapping it depends on is itself a this-year-only concept, so there's no
   "historical" version of it to reconcile against anyway.

4. **No time-series/trend fact** — deferred until the dashboard is functionally complete and accuracy
   has been raised on what's already built. Everything so far is a per-academic-year snapshot only.

5. **E1 dashboard** — deferred until E2 is complete. Will need its own `fct_e1_*` chain once E1's actual
   source system is confirmed.

## Prod deployment

Nothing built this pass has been run in prod yet. Scoped command (assumes everything these reference
that wasn't touched this session — `stg_bubble__partner`, `int_bubble__partner`, `int_dots__*`,
`int_crm__partners`, `int_pc_user_data`, `int_google_sheet__chapter_mapping`, etc. — already exists in
prod from regular runs):

```
dbt run --target prod --select \
  int_bubble__academic_year int_bubble__class int_bubble__school_holiday \
  int_bubble__slot int_bubble__school_class int_bubble__school_volunteer \
  dim_chapter_mapping dim_school_academic_year_status \
  fct_e2_volunteer_allocation_history fct_e2_child_allocation_history \
  fct_e2_volunteer_attendance_by_slot_date fct_e2_child_attendance_by_slot_date \
  fct_e2_cancellations fct_e2_volunteer_consistency fct_e2_child_consistency \
  fct_e2_school_coverage fct_e2_sessions_summary fct_e2_volunteer_recruitment \
  prod_e2_dashboard_summary prod_e2_volunteer_consistency prod_e2_child_consistency
```

Re-run this doc's "Prod deployment" section (updating the model list) after every further change so the
command stays accurate.
