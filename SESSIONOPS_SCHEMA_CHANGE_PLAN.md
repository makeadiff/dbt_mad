# Session-Ops Schema Change Plan

Source: a review of `sessionops_schema_new.md` against the learnings in
`SCHEMA_GAPS_AND_LEARNINGS.md` (gaps found building the E2 dashboard on the old Bubble schema).
Every item below was worked through and confirmed in discussion on 2026-08-20 — this is the resulting
build plan, not a proposal still up for debate. Written so it's usable two ways: as a review document
for the team, and as an implementation spec (concrete fields, triggers, and ordering) for whoever — or
whichever Claude session — builds it.

Five items solve four real problems. Ordered by dependency, not by number.

---

## 1. Auto-create `SchoolAcademicYear` for newly converted partners

**Problem it solves:** there's no flow today that creates a `SchoolAcademicYear` row for a brand-new
chapter. The tempting fix — hardcode "new chapters start in 2026-2027" somewhere in the onboarding
code — is exactly the mistake already made once (`SCHEMA_GAPS_AND_LEARNINGS.md` #2): it needs a code
change every single year. `AcademicYear.is_active` already exists as a global singleton (exactly one
row `True` at a time, enforced by `uniq_active_academic_year`), so there's no need to hardcode or even
ask a human — the correct year is always just "whichever one is active right now."

**What to build:**
- A trigger (Django signal on `Partner` post-save, or inline in whatever code path handles the CRM
  conversion sync) that fires when `Partner.converted` transitions to `True`.
- That trigger creates one `SchoolAcademicYear` row: `school_id = partner.partner_id`,
  `academic_year = AcademicYear.objects.get(is_active=True)`, `is_active=True`, `removed=False`.
- Make it idempotent — check for an existing non-removed row for `(school_id, academic_year_id)`
  before creating (the model's own `unique (school_id, academic_year_id) where removed=False`
  constraint will reject a duplicate anyway, but the trigger should check first rather than rely on
  catching that error).
- No new fields, no migration. Pure workflow addition.

**Depends on:** nothing. Build first.

---

## 2. Add `school_academic_year_id` to `ClassSection`

**Problem it solves:** `ClassSection`'s only path to "which academic year is this" today is through
`school_class_id` (nullable FK) → `SchoolClass` → `SchoolAcademicYear`. A section created independently
of a `school_class` (a bucket-slug section, per the M6 pivot) has **no path to a year at all**. This is
why "how many sections/mentoring circles existed last AY vs. this AY" can't be answered today — for
those sections there's nothing to group by.

**What to build:**
- Migration: add `school_academic_year_id` (`FK → SchoolAcademicYear`, nullable, `on_delete=PROTECT`
  to match the project's FK convention) to `ClassSection`.
- Populate it at creation time in every code path that creates a `ClassSection` row — regardless of
  whether `school_class_id` is set. For school_class-linked sections this will just mirror the parent
  `SchoolClass.school_academic_year_id`; for bucket sections it's the only source of truth.
- **Backfill**: existing `ClassSection` rows (particularly historical bucket ones) won't have this
  populated. *(Confirmed handled separately — product/ops is prepopulating the historical/last-year
  rows directly; engineering doesn't need to build a backfill script for this.)*

**Depends on:** nothing structurally, but item 3 depends on this existing and being populated first.

---

## 3. Independent progression flow for bucket-slug `ClassSection`s

**Problem it solves:** a genuinely new gap, not a port of an existing bug. In Bubble, bucket-style
sections implicitly progressed alongside their `school_class`. In session-ops, `ClassSection` was
deliberately decoupled from `SchoolClass` (child enrollment now requires a class but not a section) —
which is the right call for enrollment, but it means bucket sections (`school_class_id IS NULL`) have
**no progression mechanism at all** now. Left alone, they'd just sit stale forever instead of rolling
into the new year the way every other progressed entity does.

**What to build:**
- A new step in (or run alongside) the existing chapter/year progression job. For each school being
  progressed: find every `ClassSection` where `school_class_id IS NULL`, `is_active=True`,
  `removed=False`, and `school_academic_year_id = <old year>`.
- For each one found: retire it (`removed=True`, `deleted_at=now()`) and create its successor —
  same `section_code`/`section_name`/`section_display_name`/`school_id`, new
  `school_academic_year_id = <new year>`, `is_active=True`.
- This mirrors exactly what already happens to `SchoolClass` at progression — the only difference is
  it has to be its own explicit step, since it's no longer a side effect of `SchoolClass` progressing.

**Depends on:** item 2 (needs `school_academic_year_id` populated to know which rows belong to "this
year" and to stamp the successor correctly).

---

## 4. `SchoolVolunteer` — append/retire (SCD-2) instead of update-in-place

**Problem it solves:** `SchoolVolunteer` is a "one volunteer per school" dedup record — current state
only. Updating it in place (as happens today) means a continuing volunteer's row just gets silently
overwritten each time something changes, destroying any way to answer "how many volunteers were at this
school last year vs. this year." It was also the one entity left out of the existing year-end
progression job that already closes out `SchoolClass`/`Slot`/`ChildClass` — which is exactly why it
only ever reflects "now."

**What to build — two triggers, both must set `school_academic_year_id` correctly (the field already
exists on the model, nullable — this is about actually using it, not adding it):**

1. **Worknode move**: when a `User.worknode_id` changes to point to a *different* school (detected in
   the existing worknode sync workflow), retire the volunteer's current active `SchoolVolunteer` row
   (`removed=True`, `deleted_at=now()`) and create a new row for the destination school, with
   `school_academic_year_id` set to that school's currently-active `SchoolAcademicYear`.
2. **Year-end progression**: add `SchoolVolunteer` into the existing chapter progression job — retire
   every currently-active `SchoolVolunteer` row for that school (`removed=True`, `deleted_at=now()`).
   Do **not** auto-recreate a row for the new year — that would break the deliberate product decision
   that volunteering doesn't auto-continue. A new row only gets created if/when the sync subsequently
   reconfirms that volunteer for the school in the new year, at which point that row's
   `school_academic_year_id` gets set to the new year.

No new table (a separate `BatchVolunteer`-style history table was considered and rejected — this
achieves the same result inside the existing table and its existing "one active row per volunteer"
constraint). No new fields — `school_academic_year_id` already exists on `SchoolVolunteer`; this item is
entirely about wiring it into these two triggers correctly.

**Depends on:** nothing structurally; can build in parallel with items 2/3.
 
---

## 5. `is_active`/`removed` consistency enforcement (recommended, not blocking)

**Problem it solves:** the two-boolean pattern is repeated across most domain models. The *definitions*
are already clear and agreed — this isn't gap #1's original problem (undocumented meaning) — but nothing
currently stops an invalid combination from being written by a bug in some future workflow.

**What to build:** a short audit of which models are actually at risk of this (start with the ones the
old Bubble build found real trouble on: `school_academic_year`-equivalent, `child_class`,
`slot_class_section_volunteer`), then add DB-level `CHECK` constraints or model-level validation so an
invalid write becomes a hard error instead of a silent drift. Scope this after the audit — don't
blanket-add constraints to all 15+ models with the two-boolean pattern without first checking which are
actually exposed to the risk in the new tool's workflow design (some may already be safe by
construction).

**Depends on:** nothing. Lowest priority — do after 1–4 land and are stable.

---

## Confirmed NOT changing — don't redo these

So a reviewer or implementer doesn't second-guess decisions already made:

- **No `rolled_over_to_id`** on `SchoolAcademicYear` — redundant given `AcademicYear.is_active`'s global
  singleton plus the existing `unique(school_id, academic_year_id) where removed=False` constraint.
  "Which row is current" is already fully deterministic.
- **No `deleted_by` field** — confirmed `updated_by` is never touched after `deleted_at` is set, so it
  already answers "who deleted this" with no ambiguity.
- **No merging `child_class`/`child_class_section`** — the decoupling (class mandatory, section
  optional) was deliberate and is staying; the two entities have genuinely independent lifecycles.
- **No new `engine`/`chapter_status`/`city` fields** — already present (`PartnerWorknode`, `Partner`),
  and CRM-owned where relevant; not session-ops's schema to change.
- **Chapter exited-vs-not-yet-progressed ambiguity** — already resolved structurally by the CRM removal
  cascade (a partner removed in CRM cascades to a full soft-delete in session-ops), which is a
  completely different signal from "hasn't been progressed to the new AY yet." No schema change needed
  here — just don't try to infer chapter status from `SchoolAcademicYear` presence/absence alone.

---

## Build order

```
Phase 1 (independent, build first)
 ├─ 1. Auto-create SchoolAcademicYear on partner conversion
 └─ 2. Add school_academic_year_id to ClassSection (+ migration)

Phase 2 (depends on Phase 1; can run in parallel with each other)
 ├─ 3. Bucket ClassSection progression flow          (depends on 2)
 └─ 4. SchoolVolunteer append/retire + year stamping  (depends on nothing above, but logically pairs with 3)

Phase 3 (after 1–4 are stable)
 └─ 5. is_active/removed consistency audit + constraints
```

## What this plan does *not* cover

`SCHEMA_GAPS_AND_LEARNINGS.md` documents two items still open beyond this plan's scope: a distinct
"session occurred" event separate from attendance logging (gap #7), and the general
enrolled→awaiting_slot→scheduled pipeline-state idea (gap #8). Neither came up as concrete, agreed
session-ops work in this round of discussion — they're recorded there as design direction, not
scheduled here.
