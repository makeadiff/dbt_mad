# Schema Design Gaps — Learnings from Building the E2 Dashboard   

This document exists for one reason: while building the E2 chapter dashboard on top of the *current*
tool's schema (Bubble + DOTS + the Google Sheet ops mapping), we kept hitting confusion, rework, and
wrong assumptions that traced back to gaps in how the underlying schema models certain concepts — not
to mistakes in the dbt models themselves. Since a similar tool is being rebuilt on a different stack
with a *similar* schema, reusing the same schema as-is would just carry every one of these gaps forward.
This is the list to review before finalizing that new schema, so the same confusion doesn't repeat.

Each entry: what we observed, why it caused real trouble, and what a better schema would do instead.

---

## 1. `is_active` + `is_removed` as two independent booleans, with entity-specific meaning nobody documented

Nearly every entity (`school_academic_year`, `school_volunteer`, `slot_class_section_volunteer`, `slot`,
`school_class`, ...) carries both an `is_active` and an `is_removed` flag, but their *combined* meaning
is different on every entity, and had to be reverse-engineered from real data each time — never assumed
correctly on the first try:

- On `school_academic_year`: `is_active=false, is_removed=false` turned out to mean "this year has
  been rolled over / closed out" (a completely normal, healthy state), **not** "this record is
  invalid" or "dropped out." We initially assumed the opposite.
- On `school_volunteer`: we assumed `is_active=false, is_removed=false` meant "archived last year's
  recruit." That combination **doesn't exist at all** in the real data — only `is_removed=true` or
  `is_active=true, is_removed=false` actually occur.
- On `slot_class_section_volunteer`: only 7 of 1,588 rows are `is_active=true, is_removed=false`
  (genuinely, currently active); 1,183 are formally removed and 398 are deactivated-without-removal —
  three very different real-world situations living behind the same two flags, with no documentation
  anywhere saying what triggers each combination.
- Even *within* the same "archive last year's records" event, the flag doesn't flip reliably across
  every entity. `slot`, `school_class`, and `slot_class_section_volunteer` cleanly flip `is_active` to
  `false` once a year closes (near-100% consistent). `child_class` does **not** — 29% of `child_class`
  rows tied to an archived year are still sitting at `is_active=true`. Same rollover event, same schema
  pattern, unreliable on one entity and reliable on the others — meaning even a branching rule that
  correctly handles one entity's archival behavior can silently undercount on another, and there's no
  way to know which entities are reliable without checking real data each time.

**Better schema**: use a single explicit status/lifecycle field with named states (e.g.
`status: enum('active', 'archived', 'removed', 'pending')`) instead of two independently-toggleable
booleans whose cross-product meaning is implicit and inconsistent across tables. If two flags are kept
for legacy/audit reasons, document per-entity what each of the four combinations means, and add a
CHECK constraint or trigger so combinations that shouldn't exist can't be written.

## 2. No field anywhere says "this is the current/latest academic year"

Every place we needed "the current academic year" or "the end-of-year cutoff date," we ended up
hardcoding it directly into SQL (`when academic_year = '2025-2026' then '2026-04-30'`, `when
academic_year = '2026-2027' then '2027-04-30'`, ...). This means **every new academic year requires a
code change** in multiple models, rather than being read from data. There's also no field that says
"school X's most recent academic-year record is Y" — we had to build that ourselves
(`dim_chapter_current_status`) by ranking records, because nothing upstream provides it.

**Better schema**: an `academic_year` table with explicit `start_date`, `end_date`, and an
`is_current` flag (exactly one row true at a time). Every derived "which year is this" or "is this
year over yet" computation should read those columns, never a hardcoded date literal in downstream code.

## 3. "Is this chapter currently active" has no field — it's reconstructed, badly, by a side-channel spreadsheet

This is arguably the most consequential gap. There is no first-class "is this chapter/school currently
active in the program" field anywhere in Bubble. The org's workaround is a **human-maintained Google
Sheet** (the "ops mapping sheet") with its own `Chapter_Status` column (`Active` / `Dropped out`) —
which we found has serious problems of its own (see #4). Meanwhile, the *closest* Bubble-native signal
(a school's latest `school_academic_year` record) can't actually answer "did this chapter leave the
program" on its own — see the whole `dim_chapter_current_status` saga in this build for why (a chapter's
old year showing "inactive" is normal rollover 91% of the time, not a program exit).

**Better schema**: a first-class `chapter_status` (or `partner_status`) field with explicit lifecycle
states (`onboarding`, `active`, `paused`, `exited`) maintained as part of the actual system-of-record
workflow (e.g. set explicitly when a CO/ops person marks a chapter as exited), not inferred after the
fact from unrelated per-year archival flags, and not delegated to a spreadsheet that nothing enforces
consistency with.

## 4. The ops mapping sheet duplicates Bubble's chapter/partner concept, and the two disagree

The Google Sheet used for chapter status, CO/CHO assignment, engine (E1/E2), and city — none of which
exist natively in Bubble — is entirely human-maintained, with real consequences:
- Of 561 Bubble partners, **454 have no corresponding row in the sheet at all**.
- `chapter_name` disagrees between Bubble and the sheet for a real subset of chapters — including two
  chapters (`"st. xaviers school"` / `"st. xaviers school 2"`) that appear to have their names **swapped**
  between the two sources.
- The sheet has no history — it only reflects today's status, so there's no way to know what a
  chapter's status *was* in a past academic year from the sheet alone.

**Better schema**: whatever the new stack does for chapter/partner metadata that Bubble-equivalent
doesn't natively support (CO/CHO assignment, program engine, status) should live in the same
system-of-record as everything else, with proper foreign keys back to the canonical partner/chapter
entity — not a parallel spreadsheet with no referential integrity and no reconciliation process.

## 5. A school can have multiple `school_academic_year` records for the *same* (school, year) pair

Bubble allows more than one `school_academic_year_id` to exist for the same `(school_id,
academic_year_id)` business key over time (e.g. recreated after being removed once). There's no
"supersedes" or "version" pointer between them — the only way to find the authoritative one is to pick
whichever was modified most recently, which we had to build ourselves
(`dim_school_academic_year_status`'s `distinct on (school_id, academic_year_id) order by modified_date
desc` logic).

**Better schema**: enforce a real uniqueness constraint on `(school_id, academic_year_id)`, or if
re-creation after removal is a legitimate workflow, make it explicit with a `superseded_by_id` /
`version` column so "which one is current" is a stored fact, not an inference.

## 6. No FK resolution from `slot`/`school_class` down to academic year was actually wired through

Bubble's raw tables *do* carry a `school_academic_year_id` on both `slot` and `school_class`, but the
existing derived/staging models had silently dropped that column before this build started — we had to
add it back in ourselves. This suggests "which academic year does this record belong to" wasn't treated
as a first-class relationship worth preserving when those models were originally built.

**Better schema**: not a schema issue exactly, more a modeling discipline one — but worth noting for
the new stack's pipeline: any FK that exists on a raw/source table should be carried through every
derived layer by default, not dropped unless there's a specific reason.

## 7. No explicit "session occurred" event, distinct from "someone logged attendance for it"

The only way to know a session actually happened is if a volunteer's DOTS attendance submission exists
for that date. There's no separate "session occurred" record independent of attendance logging. In
practice, DOTS attendance data is **extremely sparse** relative to what the schedule implies should
exist (564 total attendance rows across the whole program, vs. a date-math-derived "planned sessions"
figure that sums into the hundreds per chapter) — so a genuinely-run session with nobody remembering to
log attendance is indistinguishable from a session that never happened at all.

**Better schema**: a distinct "session instance" entity (one row per scheduled occurrence of a slot),
created proactively from the schedule rather than reactively from attendance logs, with attendance as a
separate thing that gets attached to it. That would make "planned vs. happened vs. logged" three
answerable, distinct questions instead of collapsing the second two into one unreliable signal.

## 8. Enrollment and slot-scheduling are disconnected, with a large silent drop-off between them

A child can be enrolled in a class_section in Bubble with no `slot_class_section` ever created for that
section — meaning the child is "enrolled" but has no way to ever get attendance tracked. We measured
this directly: of 3,365 actively-enrolled children, only 1,535 (46%) are in a section that has an
actual scheduled slot. Same shape of gap on the volunteer side: of 4,353 volunteers ever recruited, only
1,239 (28%) were ever assigned to a slot at all. There's no intermediate state (e.g. "enrolled, pending
scheduling") that distinguishes this from either a genuine backlog or a data-modeling blind spot — right
now it just looks like the child/volunteer vanished.

**Better schema**: model enrollment and slot-assignment as an explicit pipeline with visible
intermediate states (`enrolled` → `awaiting_slot` → `scheduled` → `active`), so "not yet scheduled" is a
queryable state rather than an absence of related rows that has to be inferred by counting what's
missing.

## 9. "Is this active, or just not rolled forward yet" is the same unanswerable question at every level

This isn't one gap, it's the same gap showing up repeatedly at different levels of the hierarchy, which
is exactly why it's worth calling out as a pattern rather than three unrelated findings:

- **Chapter level**: a school active in 2025-2026 with no 2026-2027 record yet (18 such schools) —
  hasn't been rolled forward, or genuinely stopped? Can't tell from the data.
- **Child level, same shape**: of 3,225 children currently marked active on their master record, 293
  have a 2025-2026 class enrollment but no 2026-2027 one yet — same ambiguity, one level down.
- **Child level, a different flavor of the same problem**: 111 children have a perfectly valid,
  current 2026-2027 class enrollment, but their *own master record* is marked `is_active=false`. Two
  separate "is this active" signals (the child record itself vs. the enrollment record) that can
  simply disagree, with nothing reconciling them.

Every one of these took a real investigation (cross-referencing two tables, counting overlaps) to even
notice, let alone answer. None of them have a definitive answer today — "pending rollover" and "actually
dropped out" produce identical data.

**Better schema**: this reinforces gap #3's recommendation, but generalizes it — don't just add an
explicit lifecycle status to chapters, add it consistently to *every* entity that has this "still going,
or quietly stopped" ambiguity (children, volunteers, enrollments), and make rollover an explicit,
logged event ("child X's enrollment was carried forward from 2025-2026 to 2026-2027 on this date") rather
than something inferred after the fact by checking whether a new row happens to exist yet.

## 10. Two independent, overlapping-but-not-identical "is this child enrolled" tables

Bubble has both `child_class` (child → class, mandatory at enrollment) and `child_class_section` (child
→ class *section*, a more specific, optional assignment). Both are real, both get queried for "who's
enrolled," and they don't agree: for the same academic year, one misses ~54% of what the other has
depending on which schools have gotten around to creating class_sections. Nothing marks one as
authoritative or the other as derived from it — they're just two parallel, independently-maintained
facts about the same child that happen to usually overlap.

**Better schema**: if "assigned to a class" and "assigned to a specific section within that class" are
both real, meaningful states, make the second a strict refinement of the first (a child can't be in a
section without first being in the class, enforced by the schema, not by convention) — so there's one
enrollment record with a nullable "section" field, rather than two separate tables that can drift apart.

---

## How to use this

Once the E2 dashboard work is further along and there's been a chance to discuss all of the above,
this list is the input for a "what should the new schema actually do differently" conversation — not
just a reference for working around the current schema's quirks in dbt. If the new stack's schema
reproduces these same patterns, the same category of confusion (and the same amount of exploratory
querying to understand what a flag "really" means) will resurface there too.
