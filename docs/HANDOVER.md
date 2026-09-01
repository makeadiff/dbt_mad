# dbt_mad — Handover Document

**Repo:** `DalgoT4D/dbt_mad` (branch under active development: `dev_gaurav`, merges to `master`)
**Generated:** 2026-08-28, from a direct read of the repository at commit `0b26b13`. Everything below is
grounded in files that exist in the repo unless explicitly marked `[INFERRED]` or
`[NEEDS INPUT: reasoning not found in repo — confirm with outgoing dev]`.

---

## 1. Purpose

`dbt_mad` is the dbt project for **Make A Difference (MAD)**, a nonprofit. It transforms operational
data from MAD's program-delivery and CRM systems into a warehouse used for internal reporting
dashboards and to feed data into (and, increasingly, migrate data toward) other applications.

**Source → layer flow:**
`sources (raw, Airbyte-loaded)` → `bronze (1:1 staging)` → `silver (reshaped/deduped/FK-resolved)` →
`marts/core (shared dim/fact/bridge)` → `gold/analytics` + `gold/external_apps` (final consumer-facing
outputs).

**What data it transforms** (from `models/sources.yml`, 5 source systems, ~100 raw tables):
- **`bubble_raw`** — MAD's program-delivery platform ("Bubble"), covering chapters/schools, class
  sections, children, volunteers, slots, subjects, academic years. This is the system of record for
  the **E2 program engine**.
- **`dots_raw`** — a bot-based attendance-submission system (`childAttendance`, `volunteerAttendance`).
  Referred to throughout the codebase as "DOTS"; its full name/expansion is not documented anywhere in
  this repo `[NEEDS INPUT]`.
- **`crm_raw`** — the CRM system tracking partner/school onboarding: partners, MOUs, partner agreements,
  meetings, points of contact (POCs), community organizers (COs).
- **`pc_raw`** ("Platform Commons") — a separate applicant/volunteer/fundraising/class-ops platform
  (large schema: ~80 raw tables covering opportunities, applicants, fundraising donations, credits,
  batches, worknodes/hierarchy). Appears to power a different, possibly newer/parallel program engine
  (**"E1"** is referenced as the program this data serves, contrasted with E2/Bubble).
- **`opschapter_google_sheet_raw`** — a single human-maintained Google Sheet (`master_mapping_sheet`)
  used as an operational mapping layer between chapters and internal org structure (worknodes).

**Who/what consumes the output:**
- **Apache Superset dashboards** — explicitly named in `prod_e2_dashboard_status_mix.sql` ("long/unpivoted
  status breakdowns for pie, donut, and 100%-stacked-bar charts in Superset"). The `gold/analytics`
  layer (`prod_e2_dashboard_summary`, `prod_e2_dashboard_status_mix`, `prod_e2_child_consistency`,
  `prod_e2_volunteer_consistency`, `prod_sric_dashboard_data`, `prod_class_ops_data`,
  `prod_school_data_comparison`, `prod_volunteer_recruitment`, `prod_user_school_mapping`,
  `prod_school_volunteer_children`) exists specifically to feed these dashboards.
- **A new "session-ops" platform** — a Django-based application that a large chunk of
  `gold/external_apps` (~20 `prod_*_migration.sql` models) is actively migrating Bubble data *into*.
  This is a one-time/ongoing data migration project, documented in `SESSIONOPS_SCHEMA_CHANGE_PLAN.md`
  and `sessionops_schema_new.md`.
- **Salesforce** — `prod_sf_user_and_chapter_data.sql` and `prod_sf_fundraising_data.sql` produce
  Salesforce-shaped feeds (chapter/fundraiser/CO/CHO rosters, fundraising data).
- **Other external apps** — `gold/external_apps` also includes app-specific feeds for chapter mapping,
  class-ops, partner/child data, volunteer allocation history, etc. — one model per consuming
  app/integration, per this project's own layering convention.

**Why it exists (business purpose):** MAD runs volunteer-delivered educational programs across many
schools/chapters. Program delivery is tracked in Bubble (E2) and a separate Platform Commons system
(E1-adjacent), attendance is submitted via a bot (DOTS), and partner/donor relationships are tracked in
a CRM. None of these systems talk to each other natively. This warehouse exists to (a) give ops teams a
single, chapter-level view of program health (coverage, attendance consistency, volunteer recruitment)
via Superset dashboards, and (b) now, additionally, to migrate/consolidate Bubble's data into a new
unified session-ops platform. `DIMENSIONAL_MODEL_PROPOSAL.md` documents the original motivation for
adopting a star-schema layering: before this, reporting logic (100–400 line CTEs) was duplicated and
inconsistent across "prod" models with no shared semantic layer.

---

## 2. Architecture

### Layer structure and model counts

Confirmed by direct file count (`models/**/*.sql` + `models/sources.yml` = 260 files total):

| Layer | Path | Models | Materialization | Naming |
|---|---|---|---|---|
| Sources | `models/sources.yml` | 5 source defs, ~100 raw tables, **0 columns/tests/descriptions declared** | n/a | n/a |
| Bronze (staging) | `models/bronze/{bubble,crm,dots,platform_commons,google_sheet}` | **113** | `table` | `stg_<source>__<table>.sql` |
| Silver (intermediate) | `models/silver/{bubble,crm,dots,platform_commons,google_sheet}` | **49** | `table` | `int_<source>__<entity>.sql` |
| Marts/core | `models/marts/core/{dimensions,facts,bridges}` | **50** (19 dims, 28 facts, 3 bridges) | `table` | `dim_/fct_/bridge_<entity>.sql` |
| Gold/analytics | `models/gold/analytics` | **10** | `table` | `prod_<report>.sql` |
| Gold/external_apps | `models/gold/external_apps` | **37** | `table` | `prod_<app_feed>.sql` |

(`bronze` folder also technically covers a `dots` subfolder not explicitly listed in `dbt_project.yml`'s
schema config — see Known Issues.)

Layer conventions (naming, what belongs where, when to skip a layer) are documented in
`.claude/skills/dbt-modeling-conventions/SKILL.md` — read that before adding or moving any model.

### Key model lineage — the most important/complex chains

**A. E2 chapter ops dashboard chain** (Bubble + DOTS → Superset; the largest, most-recently-built chain,
per `E2_DASHBOARD_NOTES.md`):
```
stg_bubble__{academic_year,class,school_holiday,slot,school_class,school_volunteer,partner,children,...}
stg_dots__{child_attendance,volunteer_attendance}
  → int_bubble__{academic_year,class,school_holiday,slot,school_class,school_volunteer,children,...}
  → int_dots__{child_attendance,volunteer_attendance}
  → int_google_sheet__chapter_mapping (from stg_google_sheet__master_mapping_sheet)
  → marts/core dims: dim_chapter_mapping, dim_chapter_current_status, dim_school_academic_year_status
  → marts/core facts: fct_e2_volunteer_allocation_history, fct_e2_child_allocation_history,
      fct_e2_volunteer_attendance_by_slot_date, fct_e2_child_attendance_by_slot_date,
      fct_e2_cancellations, fct_e2_cancellation_reasons, fct_e2_volunteer_consistency,
      fct_e2_child_consistency, fct_e2_school_coverage, fct_e2_sessions_summary,
      fct_e2_volunteer_recruitment
  → gold/analytics: prod_e2_dashboard_summary (the main rollup), prod_e2_dashboard_status_mix
      (unpivoted for Superset pie/bar charts), prod_e2_child_consistency, prod_e2_volunteer_consistency
      (standalone per-row dashboards, deliberately siblings of prod_e2_dashboard_summary, not built on
      top of it — a change to one doesn't ripple into the other), prod_sric_dashboard_data
```
This chain is unusually well-documented: every fact/dim header comment explains grain, data-quality
caveats, and specific confirmed row counts (e.g. `fct_e2_school_coverage.sql` has ~50 lines of comments
explaining why `is_active`/`is_removed` filtering differs by column, backed by specific confirmed
counts like "2,061 children warehouse-wide have `is_active=false`... but a stale `is_active=true`
`child_class` row"). Treat these comments as load-bearing documentation, not decoration.

**B. Session-ops migration chain** (Bubble raw → new Django platform, one-time/ongoing data migration):
```
bubble_raw.{academic_year,class,program,school_academic_year,school_class,class_section,
  class_section_subject,child,child_class,child_class_section,child_program,child_subject,
  batch_child,child_removal_log,school_holiday,school_session_detail,school_volunteer,slot,
  slot_class_section,slot_class_section_volunteer}
  → prod_academic_year_migration, prod_class_migration, prod_program_migration,
    prod_school_academic_year_migration, prod_school_class_migration, prod_class_section_migration,
    prod_class_section_subject_migration, prod_child_migration, prod_child_class_migration,
    prod_child_class_section_migration, prod_child_program_migration, prod_child_subject_migration,
    prod_batch_child_migration, prod_child_removal_log_migration, prod_school_holiday_migration,
    prod_school_session_details_migration, prod_school_volunteer_migration, prod_slot_migration,
    prod_slot_class_section_migration, prod_slot_class_section_volunteer_migration
```
Notable pattern across every one of these ~20 models (all documented per-file, confirmed by reading
several in full):
- Sourced **directly from `bubble_raw`**, not from `stg_bubble__*`, because bronze casts
  `Created_Date`/`Modified_Date` to `::date`, discarding the time-of-day precision the target's
  `TIMESTAMPTZ` columns need.
- A shared **"admin fallback" convention**: any unresolved `created_by`/`updated_by` UUID falls back to
  a hardcoded `user_id 477022` ("admin"), consistently across all migration models.
- Rows that violate the *target's* required-FK/NOT-NULL constraints are **excluded from the SELECT
  output** (not migrated with a broken FK), with exact counts documented in the header (e.g. "33 rows
  have a NULL `program_id`... fall back to `program_id 1`", "47 rows... NULL `class_section_id`...
  excluded here"). There is **no separate quarantine/rejected-rows table** — see Known Issues.
- Deduplication (`dbt_utils.deduplicate`, latest by `Modified_Date`) is applied even where current data
  shows no duplicates, "for safety" / for future Airbyte re-syncs.
- `prod_school_academic_year_migration.sql` documents an explicit **load-order dependency**: it must be
  loaded into the target *after* `prod_academic_year_migration`, since it's a hard FK dependency for
  three downstream target tables.
- This chain's decisions are formally specified in `SESSIONOPS_SCHEMA_CHANGE_PLAN.md` (5 confirmed
  schema-change items, dependency-ordered) and cross-referenced against `SCHEMA_GAPS_AND_LEARNINGS.md`.

**C. Platform Commons (PC) chain** (volunteer lifecycle: applications → credits → attendance →
retention → fundraising):
```
stg_pc_* (~74 models: opportunity, opportunityApplicant, person, batchStudent, worknode, credit
  histories, donor/payment, etc.)
  → int_pc_applicant_data, int_pc_applicant_policy_status (CPP/COC policy signing status — flagged
      as an UNVERIFIED assumption about raw truthy values, see Known Issues), int_pc_user_data,
      int_pc_school_id, int_pc_class_ops_master, int_pc_credit_data, int_pc_child_attendance,
      int_pc_child_batch_enrollment, int_pc_volunteer_attendance, int_pc_volunteer_retention,
      int_pc_fellow_applicant_data, int_pc_students_data, int_pc_fundraising_donations, int_pc_events_data
  → marts/core: dim_pc_applicant, dim_pc_class, dim_pc_donor, dim_pc_student, dim_pc_volunteer,
      fct_pc_applications, fct_pc_child_attendance, fct_pc_class_sessions, fct_pc_credits,
      fct_pc_donations, fct_pc_events, fct_pc_fellow_applications, fct_pc_volunteer_attendance,
      fct_pc_volunteer_retention
  → gold: prod_class_ops_credit_data, prod_class_ops_data_ext, prod_fundraising_donations,
      prod_volunteer_fundraising_summary, prod_user_data
```
Several PC intermediate models reconstruct multi-hop join paths that aren't obvious from column names
alone (e.g. `int_pc_volunteer_attendance.sql`: `communityMemberAttendance.communityMemberId →
communityMember.id → actor.id → actor.actorId → user_id`) — read the header/inline comments before
modifying, they encode join logic discovered through investigation, not obvious from the schema.

**D. CRM / partner chain:**
```
stg_crm__{partners,cities,states,mous,partner_agreements,meetings,pocs,poc_partners,partner_cos,
  manager_co}
  → int_crm__{active_partners,cities,partners,mous,partner_agreements,meetings,pocs,poc_partners,
      partner_cos,states}
  → marts/core: dim_crm_partner, dim_mou, dim_poc, bridge_partner_co, fct_mou, fct_meetings,
      fct_partner_agreements
  → gold: prod_partner_data, prod_partner_mou_status, prod_partner_child_data,
      prod_school_data_comparison(_ext), prod_chapter_mapping, prod_sf_user_and_chapter_data,
      prod_sf_fundraising_data
```

**E. Dimensional-model reference layer** (Bubble core entities used across chains A/B):
```
stg_bubble__{children,partner,class,school_class,slot,subject,user,program} + int_bubble__* equivalents
  → dim_child, dim_bubble_partner, dim_class_section, dim_slot, dim_subject, dim_volunteer,
    dim_program, fct_child_class_section, fct_child_enrollment, fct_school_volunteer,
    fct_volunteer_assignments, fct_volunteer_slot_assignment, bridge_child_class_section,
    bridge_child_subject
```
`fct_child_enrollment.sql` is worth flagging specifically: its header documents that it was built to
reconcile two disagreeing Bubble enrollment tables (`child_class` vs. `child_class_section` — one
misses ~54% of what the other has) and is explicitly a **dbt-layer workaround**, "not a substitute for
fixing it at the source." Existing dashboards were deliberately left on the old tables pending a
separate migration decision (per `SCHEMA_GAPS_AND_LEARNINGS.md` gap #10 final status).

### Data validation pattern — what actually exists (and what doesn't)

There is **no literal "raw → validated → clean" quarantine-table pattern** (no status column, no
separate rejected-rows table anywhere in the repo — confirmed by grepping for
`quarantine|invalid_reason|validation_status` across all `.sql` files, no hits outside unrelated
`chapter_mapping`/"validation" naming). What the project actually does, across two different
mechanisms, is:

1. **Bronze → silver as the general-purpose cleaning stage.** Bronze is a strict 1:1 raw mirror
   (rename/cast only, no filtering, no joins). Silver is where `dbt_utils.deduplicate`, UUID→integer FK
   resolution, and business-rule filters (e.g. the widespread
   `where "xIsDeleted" is false or "xIsDeleted" is null` soft-delete pattern) actually happen. This is
   the project's real "clean" layer — there's no additional intermediate "validated" stage between them.
2. **The sessionops migration models' exclude-and-document pattern** (chain B above) is the closest
   thing to row-level quarantine: each model checks the *target* system's constraints (NOT NULL FKs,
   uniqueness) against the current Bubble data, and any row that would violate them is **silently
   dropped from the model's `select`** rather than migrated — with the count and reasoning written
   into the model's header comment as a point-in-time finding (e.g. "31 of 1112 rows... excluded").
   **There is no table anywhere that captures the excluded rows** — if you need to know what got
   dropped and why, you have to re-read (or re-derive) the header comment logic against current data.
   This is a real gap if the excluded-row set needs to be audited or reviewed by someone other than a
   dbt developer — see Known Issues.

### External dependencies

- **Warehouse:** PostgreSQL, hosted on AWS RDS (`mad-dalgo-warehouse...rds.amazonaws.com`, `ap-south-1`),
  database `mad_data_warehouse`. Single database with `dev`/`prod` distinguished only by **schema
  prefix** (`dev_*` vs `prod_*`), not by separate databases or credentials — both `dev` and `prod`
  targets in `profiles.yml` currently use the **same** host/db/user/password.
- **Source systems:** Bubble (program delivery, E2), Platform Commons (E1-adjacent applicant/volunteer/
  fundraising platform), a CRM system, a bot-based attendance system ("DOTS"), and one Google Sheet. All
  are loaded into the warehouse via **Airbyte** (confirmed by `_airbyte_*` metadata columns present on
  every bronze table, and by the modeling-conventions skill's explicit note that bronze must be
  materialized as `table` — not `view` — specifically so a live view doesn't block Airbyte's
  full-refresh `DROP TABLE`/recreate cycle on the raw tables).
- **git remote:** `https://github.com/DalgoT4D/dbt_mad.git` — the `DalgoT4D` org strongly suggests this
  project runs on/alongside **Dalgo**, an open-source data-platform product for nonprofits (built by
  Tech4Dev) that typically wraps dbt + Airbyte + Airflow behind a UI `[INFERRED — no orchestration
  config found in this repo itself; confirm with outgoing dev or Dalgo platform admin exactly how/where
  scheduled runs are configured]`.

### Custom macros, packages, and non-standard patterns

**Packages** (`packages.yml`):
- `dbt-labs/dbt_utils` (1.4.1) — used for `deduplicate` (silver layer) and `generate_surrogate_key`.
- `metaplane/dbt_expectations` (0.10.10) — installed; **no models currently reference it** (no hits
  searching `models/` for `dbt_expectations`) — see Known Issues.
- `elementary-data/elementary` (0.25.0) — data observability/test-result-storage package, given its own
  schema (`+schema: "elementary"`, `on_schema_change: append_new_columns` in `dbt_project.yml`).
  Confirmed **actively invoked** (1,281 log-line hits for "elementary" in the local `logs/dbt.log`), so
  this is a live part of the run, not just an installed-but-unused dependency.
- `godatadriven/dbt_date` (0.19.0) — present only in `package-lock.yml`, not `packages.yml` directly;
  a transitive dependency of one of the three above.

**Custom macros** (`macros/`):
- `generate_schema_name.sql` — overrides dbt's default schema-naming macro. Builds the actual schema
  name from the model's **folder path** (`node.fqn`) rather than a single `+schema` config value, e.g. a
  model under `models/bronze/bubble/` gets schema `<target_schema>_bronze_bubble` even without an
  explicit override — this is why `dbt_project.yml`'s bronze/silver blocks only need `+schema` set
  per-subfolder for the ones that don't map cleanly (and why the `dots` subfolder, which has no explicit
  `+schema` entry in `dbt_project.yml`, still lands in `<target>_bronze_dots`/`<target>_silver_dots`
  correctly — the macro derives it either way).
- `clean_prefix.sql` — regex-strips a Bubble-style `PREFIX.` qualifier from a column value.
- `role_code_to_label.sql` — hardcoded CASE mapping from Bubble's internal role codes
  (`role.mad.project_lead`, etc.) to human-readable labels. Will silently pass through any new role code
  Bubble introduces (falls to `else {{ column_name }}`) — needs a manual update if new roles are added.
- `macros/tests/slot_class_section_has_volunteer.sql` — one custom generic (singular) test, checking
  that every non-removed `slot_class_section` has a matching volunteer assignment. **Not currently
  referenced/applied by any model** — see Known Issues.

**Non-standard patterns worth knowing:**
- Extremely detailed **header comments as documentation-of-record** — this project relies on inline
  SQL comments (not `schema.yml` descriptions) for grain, data-quality caveats, and specific confirmed
  row counts. This is the primary source of institutional knowledge in the codebase; read them before
  changing anything.
- `mad_dbt__old_models/` — a full legacy model tree (`intermediate/`, `intermediate_aggregation/`,
  `prod/analytics/`, `prod/external_apps/`, its own `schema.yml`) sitting **outside** `models/`
  (dbt's `model-paths` is `["models"]` only), so none of it is executed by dbt. It's dead code kept
  around as reference for the "legacy model" ports mentioned throughout the header comments (e.g.
  "Ported from the legacy `fct_dashboard_summary` model"). See Known Issues.

---

## 3. Setup

### Local

**Prerequisites** (confirmed from the installed local environment):
- Python 3.13 (`.venv` built against `Python313`)
- `dbt-core` 1.10.13 + `dbt-postgres` 1.9.1 adapter
- No `requirements.txt`/`pyproject.toml` exists in the repo — dependencies were installed manually into
  a venv. `[NEEDS INPUT: confirm exact pip install command / pinned versions the outgoing dev used, or
  create a requirements.txt as part of handover]`.

**Step-by-step:**
```bash
git clone https://github.com/DalgoT4D/dbt_mad.git
cd dbt_mad

python -m venv .venv
source .venv/bin/activate        # or .venv\Scripts\activate on Windows
pip install dbt-core==1.10.13 dbt-postgres==1.9.1

dbt deps                         # installs dbt_utils, dbt_expectations, elementary, dbt_date per packages.yml

# profiles.yml already exists at the repo root (gitignored — not committed).
# dbt does NOT read a project-root profiles.yml by default; you must either:
#   a) copy/merge its `dbt_mad:` block into ~/.dbt/profiles.yml, or
#   b) pass --profiles-dir . (or set DBT_PROFILES_DIR=.) on every dbt invocation from the repo root.
# Ask the outgoing dev / your team lead for the current profiles.yml contents (see Access & Credentials).

dbt debug --profiles-dir .        # confirm the Postgres connection works
dbt build --profiles-dir .        # first full build: runs + tests everything (target defaults to `dev`)
```

**Local-only gotchas:**
- `profiles.yml` sits in the **repo root**, not the usual `~/.dbt/`. It's `.gitignore`d, so it will not
  exist after a fresh clone — you must obtain it (or its credential values) separately.
  `[NEEDS INPUT: where/how does a new team member currently get this file? No vault/secrets-manager
  reference exists in this repo — confirm with the outgoing dev.]`
- `dev` and `prod` targets in `profiles.yml` point at the **same** Postgres host, database, and
  credentials — they differ only by the `schema:` value (`dev` vs `prod`). Running against `target:
  prod` from a laptop **writes directly into the production schema** — there is no environment
  isolation beyond the schema prefix. Be deliberate about which `--target` you use.
- `seeds/`, `snapshots/`, `tests/`, `analyses/` are present but effectively empty (only `.gitkeep` /
  one throwaway file `analyses/chapter_wise_test_pilot_export.sql`) — don't expect seed data or
  snapshots to exist.
- A full `dbt build` will take a while — bronze alone is 113 tables, several sourced from a ~80-table
  raw schema (`pc_raw`).

### Production

- **No CI/CD, GitHub Actions, or scheduler config exists anywhere in this repo** (`.github/` doesn't
  exist; no Airflow DAGs, no dbt Cloud config, no cron files found in a repo-wide search).
- The git remote (`DalgoT4D/dbt_mad`) strongly suggests production runs are orchestrated by **Dalgo**
  (Tech4Dev's open-source data platform), which typically runs dbt on a schedule via its own
  Airflow-backed infrastructure, outside this repo. **This is inferred, not confirmed** —
  `[NEEDS INPUT: confirm with the outgoing dev or the Dalgo platform admin (a) where the production
  schedule/DAG is actually configured, (b) how a merge to master triggers a prod run — automatically via
  Dalgo watching the repo, or manually triggered, (c) run frequency.]`
- **Deployment process:** based on git history, the pattern is feature work on `dev_gaurav` (and other
  dev branches), merged via PR into `master`. There's no visible branch-protection or required-check
  config in-repo to confirm what gates a merge. `[NEEDS INPUT]`
- **Schedule/frequency:** not discoverable from this repo. `[NEEDS INPUT: known operational knowledge —
  ask the outgoing dev how often prod dbt runs execute (e.g. hourly/nightly) and whether Airbyte syncs
  and dbt runs are coordinated or independent.]`

---

## 4. Access & Credentials

| What | Where it lives | Notes |
|---|---|---|
| Postgres warehouse connection (host/db/user/password, `dev` + `prod` targets) | `profiles.yml` at repo root — **gitignored, not committed**, must be obtained out-of-band | Both targets currently share one host/db/user/password, differing only by schema prefix. **Do not commit this file.** `[NEEDS INPUT: where is this file's content actually distributed today — Slack, a password manager, handed over directly? No vault/secrets-manager integration found in this repo.]` |
| dbt Cloud / Dalgo platform credentials (if runs are triggered there) | Not in this repo | `[NEEDS INPUT: confirm whether prod runs go through a Dalgo UI/account, and who administers access to it.]` |
| Git/GitHub access | `github.com/DalgoT4D/dbt_mad` | Standard GitHub org membership; ask an existing org admin to grant access. |
| Google Sheet ("master mapping sheet") access | Feeds `opschapter_google_sheet_raw` via Airbyte | `[NEEDS INPUT: who owns/maintains this sheet, and what Airbyte connector/credentials sync it into the warehouse?]` |
| `.user.yml` | Repo root, contains only a dbt anonymous-usage-tracking UUID (`id: 0102232c-...`) | Not a credential — safe, no action needed. |

**Rotation:** no rotation process, expiry policy, or secrets-manager reference exists anywhere in this
repo. `[NEEDS INPUT: confirm current rotation practice with the outgoing dev — given the same
credentials cover both dev and prod, a rotation affects every developer's local setup simultaneously.]`

---

## 5. Common Operational Tasks

**Run the full project:**
```bash
dbt build --profiles-dir . --target dev     # build (run + test) everything against dev schemas
dbt build --profiles-dir . --target prod    # same, against prod — use deliberately, see Setup gotchas
```

**Run a specific model or layer:**
```bash
dbt run --select stg_bubble__children                  # one model
dbt run --select models/bronze/bubble                   # a whole folder
dbt run --select +fct_e2_school_coverage                # a model and everything upstream of it
dbt run --select fct_e2_school_coverage+                # a model and everything downstream of it
```
There are no `tag:` configs currently applied to models beyond folder-based selection — use path-based
`--select` (`models/bronze/...`, `models/gold/analytics/...`) as the primary selection method.

**Adding a new source:** add the table under the relevant source block in `models/sources.yml` (there
are currently no columns, tests, or freshness checks defined for any source — see Known Issues, so
adding one today just means adding a `name:` entry, following the existing bare-list style; consider
whether to also add proper source YAML documentation as you touch it).

**Adding a new model:** follow `.claude/skills/dbt-modeling-conventions/SKILL.md` exactly — it defines
which layer a new model belongs in, naming (`stg_<source>__<table>`, `int_<source>__<entity>`,
`dim_/fct_/bridge_<entity>`, `prod_<name>`), required header-comment content per layer, and when it's
acceptable to bypass a layer (e.g. `gold/external_apps` reaching directly into `int_`/`stg_` when a
consumer's need genuinely conflicts with a shared dim/fact's grain — always with a comment explaining
why).

**Running tests / checking coverage:** `dbt test` runs whatever's defined — which, as of this repo
state, is **effectively nothing** (no `schema.yml` test blocks anywhere in `models/`, only the unused
custom generic test in `macros/tests/`). There is no meaningful test coverage to check today; see Known
Issues for what a first testing pass should probably prioritize (primary-key uniqueness/not-null on
marts, `relationships` tests on FK-heavy migration models).

**Debugging a failed run:**
- Check `logs/dbt.log` (rotated, several 10MB+ files) for the specific error — Postgres error messages
  (constraint violations, type-cast failures) show up there directly.
- Given bronze is `table`-materialized specifically to avoid blocking Airbyte's raw-table
  drop/recreate, a bronze-layer failure most often means either (a) Airbyte hasn't finished a sync yet
  and the raw table is mid-drop, or (b) a raw column's data changed shape (see the modeling-conventions
  note about mixed ISO/epoch-millisecond timestamp strings in `pc_raw."opportunityApplicant"` as a
  concrete precedent for this failure mode).
- Elementary is installed and actively invoked — check its schema (`<target>_elementary`) for anomaly/
  test-history data if you suspect a silent data-quality regression rather than a hard run failure.

**Checking/updating the migration "quarantine":** there's no queryable quarantine table (see
Architecture). To see what a `prod_*_migration.sql` model is currently excluding, re-run its documented
checks against current `bubble_raw` data yourself (the SQL pattern is written out in each model's header
— null-required-FK checks, duplicate-ID checks) — the counts baked into the comments are point-in-time
and will drift as source data changes.

---

## 6. Known Issues / Tech Debt

Confirmed directly from the repo:

1. **Zero column-level tests or documentation anywhere in `models/`.** No `schema.yml` exists for any
   model. `models/sources.yml` itself declares only bare table names — no columns, descriptions,
   freshness checks, or tests on any of the ~100 raw source tables. `dbt_expectations` is installed but
   never referenced by any model. The only custom test (`slot_class_section_has_volunteer`) is defined
   but not applied anywhere.
2. **Credentials/environment isolation:** `profiles.yml`'s `dev` and `prod` targets use the identical
   host/database/user/password, differing only by schema prefix — there's no infrastructure boundary
   between a developer's local run and production data.
3. **No dedicated rejected/quarantine table for the sessionops migration models.** Rows that fail the
   target platform's required-FK/constraint checks are silently excluded from each `prod_*_migration.sql`
   model's output, documented only as prose in the header comment with a point-in-time row count. There's
   no way to query "what got excluded and why" later without re-deriving it.
4. **Repeated, unfinished integration TODOs** — five `gold/analytics` models carry the same open TODO:
   `prod_partner_data.sql`, `prod_class_ops_data.sql`, `prod_volunteer_recruitment.sql`,
   `prod_school_data_comparison.sql`, `prod_school_volunteer_children.sql` all say some version of
   "TODO: Integrate Platform Commons (PC) data for..." — meaning these analytics models are currently
   Bubble/CRM-only and knowingly missing Platform Commons (E1-side) data.
5. **Hardcoded placeholder logic pending real data:** `fct_e2_child_consistency.sql` and
   `fct_e2_volunteer_consistency.sql` both compute `hours_of_support`/`hours_contributed` as
   `sessions * 2` with an explicit `-- TODO: update multiplier when actual session duration per slot is
   available` — this is a known approximation, not a bug, but any dashboard built on these hour figures
   should carry that caveat.
6. **`int_pc_applicant_policy_status.sql`** documents an explicitly **unverified assumption**: CPP/COC
   policy acceptance is inferred from raw text values assumed to be `'true'/'yes'/'1'/'t'` — the header
   comment itself says this was written "with no DB access at time of writing" and should be confirmed
   against real `pc_raw."opportunityApplicantMeta"` data before being trusted in a dashboard (this
   directly feeds `prod_sric_dashboard_data.sql`'s CPP/COC-signed metrics).
7. **`mad_dbt__old_models/`** is a full legacy model tree kept in the repo outside `model-paths`, so it's
   inert to dbt but still present and could confuse a new contributor into thinking it's live code, or
   into editing it by mistake. It's referenced only as historical "ported from legacy X" context in
   header comments elsewhere.
8. **`DIMENSIONAL_MODEL_PROPOSAL.md` is now a superseded planning document, not current spec** — it
   proposed a `staging/intermediate/marts` structure; the implemented structure is
   `bronze/silver/marts/gold` (an extra `gold` layer was added beyond the original proposal). Some
   proposed models never got built as named (`dim_date`, `dim_user`, `dim_campaign`; `fct_child_attendance`,
   `fct_donations` are instead split across `pc_`-prefixed equivalents). Don't treat this file as an
   accurate description of the current model set — it's decision-log material (see Section 7), not a
   spec to reconcile against.
9. **7 of 12 items in `SCHEMA_GAPS_AND_LEARNINGS.md` remain explicitly "Open"** as of its last update
   (2026-08-20) — see that file's "Final status" table for the full list (gaps #1, #6, #7, #8, #9, #11,
   #12). These are source-system (Bubble) schema limitations that dbt models currently work around with
   documented but sometimes fragile business-rule filters — `fct_e2_school_coverage.sql` alone contains
   roughly 50 lines of comment explaining several rounds of undercounting bugs and fixes tied to these
   gaps. Any future change to `is_active`/`is_removed` filtering logic in Bubble-derived models should be
   read against this document first.
10. **`dbt_project.yml`'s bronze/silver schema config blocks don't list a `dots` subfolder** (only
    `bubble`, `crm`, `platform_commons`, `google_sheet` have explicit `+schema` entries), even though
    `models/bronze/dots/` and `models/silver/dots/` exist and build successfully — the custom
    `generate_schema_name` macro's folder-path fallback happens to produce the same schema name anyway,
    but this is an inconsistency in the config file worth cleaning up for clarity.
11. **`git status` at the time of writing shows an uncommitted, in-progress change** to
    `models/silver/platform_commons/int_pc_user_data.sql` — confirm with whoever's mid-edit before
    building on top of it.

**Institutional-knowledge placeholders — please fill in:**
- `[NEEDS INPUT: any models known to be flaky/slow in production runs, or that regularly need manual
  re-runs?]`
- `[NEEDS INPUT: any manual workarounds currently in place outside dbt — e.g. someone manually editing
  the Google Sheet mapping, manually re-triggering Airbyte syncs, manually patching data in Superset?]`
- `[NEEDS INPUT: current status/owner of the "bot attendance pilot" work mentioned in the latest commit
  (0b26b13) and the temp query file it added — is this active exploratory work or should it be cleaned
  up?]`
- `[NEEDS INPUT: is there a known cadence/dependency between Airbyte syncs and dbt runs, or do they run
  independently and occasionally race?]`

---

## 7. Decision Log

Rationale that **is** documented in the repo:

- **Why the bronze/silver/marts/gold layering exists at all:** `DIMENSIONAL_MODEL_PROPOSAL.md` states
  the prior approach ("intermediate → prod" with no shared dimensional layer) produced 100–400 line CTEs
  duplicated across "prod" models, each re-deriving things like "active children" with potentially
  different logic — no consistency guarantee, almost no tests, and no reusability. The star-schema
  layering was adopted specifically to make facts/dimensions conformed and shared.
- **Why bronze must be materialized `table`, not `view`:** `.claude/skills/dbt-modeling-conventions/
  SKILL.md` documents a real production incident — a live view over an Airbyte-loaded raw table created
  a `pg_depend` dependency that blocked Airbyte's full-refresh `DROP TABLE` on `crm_raw.cities`/
  `meetings`. Bronze was switched to `table` project-wide as a direct fix.
  `dbt_project.yml`'s own inline comment confirms this ("persisted so Airbyte's raw-table drop/recreate
  isn't blocked by view dependencies").
- **Why bronze selects every raw column, even unused ones:** the same skill file cites a specific prior
  incident of staging models silently dropping raw columns (`partner.converted`, `user.is_active`,
  `chapter.removed`) that later turned out to be needed — cheaper to carry unused columns than to
  backfill them later.
- **Why intermediate models don't exist 1:1 with staging models:** deliberate — the skill file states an
  `int_` model is only created when there's real reshaping work (dedup, FK resolution, business rules);
  a clean staging model needed as-is downstream is referenced directly. Cited example: bubble has 25
  `stg_bubble__*` models but only ~16 `int_bubble__*` models at the time that convention was written.
- **Why the sessionops migration models bypass `stg_`/`int_` and read `bubble_raw` directly:**
  documented per-model (see Architecture chain B) — bronze's `::date` cast on `Created_Date`/
  `Modified_Date` discards time-of-day precision the target platform's `TIMESTAMPTZ` columns need for
  migration fidelity. This is a deliberate, repeated exception to the normal layering convention,
  justified the same way in every migration model's header.
- **Why `fct_e2_volunteer_allocation_history` (marts/core) is separate from
  `gold/external_apps/prod_volunteer_allocation_history.sql`:** documented in `E2_DASHBOARD_NOTES.md` —
  the external_apps version is a separate, child-expanded view built for one specific app's needs and
  was deliberately left untouched rather than merged.
- **Why `dim_chapter_mapping` sources `chapter_name`/`state` with a Bubble-first, sheet-as-fallback
  policy (and drops the sheet as a CO/CHO source entirely):** documented in the model's own header and
  corroborated by `SCHEMA_GAPS_AND_LEARNINGS.md` gap #4's "Final status" — the mapping sheet is
  human-maintained and was found to have real copy-paste-grade name mismatches against Bubble, and its
  CO/CHO data added no information beyond what Bubble's own `partner.co_id` already provided via a
  cleaner join. This was a deliberate 2026-08-19 fix, not the original design.
- **Why the sessionops migration plan's 5 items were sequenced the way they are, and why 5 other
  changes were explicitly rejected** (no `rolled_over_to_id` field, no `deleted_by` field, no merging
  `child_class`/`child_class_section`, no new `engine`/`chapter_status`/`city` fields, no separate
  "session occurred" event for this round): all reasoned through explicitly in
  `SESSIONOPS_SCHEMA_CHANGE_PLAN.md`'s "Confirmed NOT changing" section — worth reading in full before
  proposing any of these as new work, since they were already considered and closed.

Where reasoning is **not** found in the repo:

- `[NEEDS INPUT: reasoning not found in repo — confirm with outgoing dev]` — why Postgres/RDS specifically
  as the warehouse (vs. a cloud warehouse like BigQuery/Snowflake, which is more typical of the Dalgo
  stack this project otherwise resembles).
- `[NEEDS INPUT: reasoning not found in repo — confirm with outgoing dev]` — why `dev` and `prod` share
  one database/credentials instead of being fully separated.
- `[NEEDS INPUT: reasoning not found in repo — confirm with outgoing dev]` — the decision to build a
  brand-new session-ops platform at all (vs. continuing to extend Bubble) — the *schema* rationale is
  extensively documented, but not the underlying product/business decision to migrate off Bubble.
- `[NEEDS INPUT: reasoning not found in repo — confirm with outgoing dev]` — why Platform Commons (E1)
  and Bubble (E2) exist as two parallel systems in the first place, and what "E1" vs "E2" actually
  designates organizationally (the codebase treats this as shared background knowledge and never
  defines it).
