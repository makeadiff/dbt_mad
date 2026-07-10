---
name: dbt-modeling-conventions
description: Layer conventions for this dbt project (bronze/silver/marts/gold) — which layer a new model belongs in, naming, materialization, required header comments, and when it's OK to bypass a layer. Use whenever creating, renaming, or moving a dbt model in models/**, or reviewing whether an existing model is in the right layer.
---

# dbt_mad modeling conventions

This project follows a Kimball-flavored layered structure (see `DIMENSIONAL_MODEL_PROPOSAL.md` for the original rationale). Four layers, each with a distinct job. Before creating a model, decide which layer it belongs in using the rules below — don't default to "just add it near similar files."

```
models/
├── bronze/            stg_<source>__<entity>.sql   — 1:1 raw mirror
├── silver/             int_<source>__<entity>.sql   — reshaped, only what's needed
├── marts/core/         dim_*/fct_*/bridge_*.sql     — shared semantic layer
└── gold/
    ├── analytics/      prod_*.sql                  — reporting, built on marts only
    └── external_apps/  prod_*.sql                   — app-specific, can be complex
```

## Layer 1 — bronze (staging)

- **One model per raw source table**, no exceptions — every table in `models/sources.yml` gets a `stg_<source>__<table>.sql`.
- Only rename + cast. **No joins, no `dbt_utils.deduplicate`, no CASE-based business logic, no filtering** (other than the standard `where "xIsDeleted" is false or "xIsDeleted" is null` pattern already used for platform_commons soft-deletes).
- Select every column that exists on the raw table, even ones with no known use yet — cheap to carry, expensive to backfill later if it turns out something needed it (this project has already been bitten by staging models silently dropping raw columns like `partner.converted`, `user.is_active`, `chapter.removed`).
- `{{ config(materialized='table') }}` — **not `view`**. Bronze used to default to `view` (see `dbt_project.yml` history), but a live view over a raw Airbyte-loaded table creates a Postgres `pg_depend` dependency that blocks Airbyte's full-refresh `DROP TABLE` on that raw table (this caused a real ingestion failure on `crm_raw.cities`/`meetings`). Always `table` here now.
- Cast defensively when a raw column is textually typed but not textually uniform — e.g. `pc_raw."opportunityApplicant".applicationCompleteDateTime` is `varchar` but mixes ISO datetime strings with epoch-millisecond strings; a blind `::timestamp` only fails once `dbt run` actually scans every row (views mask this since `CREATE VIEW` never executes the query). When a raw text column feeds a typed cast, check the actual data for mixed formats first, not just the declared type.
- Airbyte metadata columns (`_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`, `_airbyte_generation_id`) **stay in staging** — that's their natural home as ingestion audit metadata. **Do not carry them into `silver`, `marts`, or `gold`** — no intermediate/mart/analytics/external_apps model should select or expose them. It's fine to reference them *internally* within an intermediate model's CTEs (e.g. `_airbyte_extracted_at` as a `dbt_utils.deduplicate` ordering key, or `_airbyte_raw_id` as a synthetic row-identity key when the source has no reliable natural key — see `int_google_sheet__chapter_mapping.sql`), as long as they never appear in that model's final `select` output.

## Layer 2 — silver (intermediate)

- **Do NOT build one intermediate model per staging model.** Only create an `int_` model when there's real reshaping work: `dbt_utils.deduplicate`, UUID→integer FK resolution joins, or business-rule logic (CASE statements, validity flags, metric rollups). If a staging model is already clean and needed as-is downstream, reference the `stg_` model directly — don't wrap it in a pass-through `int_` model for the sake of symmetry.
  - Example: bubble has 25 `stg_bubble__*` staging models but only ~16 `int_bubble__*` intermediate models — the other 9 staging tables aren't reshaped anywhere downstream, so no intermediate model exists for them. That gap is intentional, not a missing model.
- **Every intermediate model must open with a comment block stating why it exists and what it gives downstream** — not just what it does mechanically. Follow the existing pattern, e.g. `int_bubble__children.sql`:
  ```sql
  -- Resolves UUID foreign keys for child records + deduplicates
  -- Flow: stg_bubble__children → int_bubble__child
  -- Joins: class (UUID→class_id), school_class (UUID→school_class_id), partner (UUID→school_id)
  ```
  At minimum cover: (1) the problem this model solves, (2) the upstream flow (`stg_x [+ stg_y] → int_x`), (3) what downstream consumers get that they couldn't get from staging alone.
- Materialized as `table` (folder default in `dbt_project.yml`).

## Layer 3 — marts/core (dimensions, facts, bridges)

- This is the **single shared semantic layer**. `dim_<entity>.sql`, `fct_<entity>.sql`, `bridge_<entity_a>_<entity_b>.sql`. Materialized `table`, schema `gold_marts`.
- Every dim/fact header comment must state the **grain** — what one row represents — e.g. `fct_school_volunteer.sql`:
  ```sql
  -- fct_school_volunteer: One row per volunteer assigned to one school
  -- Grain: One record per volunteer assigned to one school in one academic year
  ```
- **Treat filters and grain on these models as a public contract.** Every dim/fact here is referenced by multiple downstream models (analytics, external_apps, other facts). Changing a filter (e.g. removing `where is_removed = false` from `dim_child`) to satisfy one new consumer silently changes the output of every *other* model already built on it. Before editing an existing dim/fact's filter, grain, or column set, check `grep -rl "ref('<model>')" models/` for every consumer and confirm the change is safe for all of them — not just the one that prompted the change.
- If a new consumer's need genuinely conflicts with an existing dim/fact's filter/grain (e.g. needs removed records that the dim excludes), **do not loosen the shared dim** — instead source directly from the underlying `int_` model in that one consumer (see the External Apps rule below), or, if the need is likely to recur, add a clearly-named sibling model (e.g. `dim_child_all`) rather than mutating the shared one.

## Layer 4a — gold/analytics

- Analytics/reporting models here should build on **`dim_`/`fct_`/`bridge_` from `marts/core` only**. Don't reach into `int_`/`stg_` models directly from analytics — that's what the marts layer exists to prevent (repeat the "who's active", "what's the grain" logic once, in one place).
- Keep these models to straightforward joins across facts and dimensions. If a query needs heavy CTE logic, multi-step date parsing, or nontrivial reshaping to work, that logic almost always belongs one layer down (in `silver`, or as a new dim/fact in `marts/core`) — don't let analytics models regrow into 200-line CTEs. That complexity was the entire problem `DIMENSIONAL_MODEL_PROPOSAL.md` was written to solve.
  - Note: a handful of existing `gold/analytics` models currently reach into `int_`/`stg_` directly (pre-dating this convention) — that's known debt, not a pattern to copy for new models.

## Layer 4b — gold/external_apps

- These models feed specific external applications/integrations, one model per app-facing view. They're leaf nodes — nothing else in the DAG depends on them — so it's acceptable for them to be more complex (multiple `LEFT JOIN LATERAL`s, app-specific column renaming/shaping) than an analytics model would be.
- **Never modify a shared `dim_`/`fct_` to fit one external app's requirement.** If the app needs a filter or grain the dim/fact doesn't provide (e.g. needs removed/inactive rows that the dim excludes), source that specific piece straight from the `int_` (or even `stg_`) model instead, and **say why in a comment**, e.g. `prod_child_master_data_ext.sql`:
  ```sql
  -- Sourced from int_bubble__children (not dim_child) so removed children are still
  -- included here, matching the old child_int model's behavior.
  ```
- Still prefer pushing reusable logic down a layer when there's a clean opportunity to: if the same reshaping would help more than one external app or analytics model, put it in `silver` (or promote it into `marts/core`) rather than duplicating it across multiple `external_apps` models. Only keep it local to one `external_apps` model when it's genuinely one-off.

## Quick decision checklist for a new model

1. Does it read one raw source table with only renames/casts? → `bronze`, `stg_<source>__<table>.sql`, `table`.
2. Does it dedupe, resolve UUID FKs, or apply business rules, and is the result actually needed by something downstream? → `silver`, `int_<source>__<entity>.sql`, `table`, header comment required.
3. Is it a reusable business entity/event that multiple things will query (dim/fact/bridge)? → `marts/core`, `table`, grain comment required, treat as a shared contract.
4. Is it a report/aggregation for internal analytics, buildable from existing dims/facts alone? → `gold/analytics`, joins on marts only.
5. Is it feeding one specific external app/integration, possibly needing a filter or grain a shared dim/fact doesn't offer? → `gold/external_apps`, can go around marts to `int_`/`stg_` directly if justified in a comment.
