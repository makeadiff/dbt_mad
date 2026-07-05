---
name: dbt-model-tests
description: Prompt for grain/PK, non-null columns, and FK relationships and add dbt tests to schema.yml whenever a dbt model (.sql under models/) is created or modified. Use immediately after any dbt model edit, before summarizing the turn, unless the model is ephemeral or disabled. Ensures every model gets baseline not_null + unique on its PK and relationships tests on its FKs.
---

# dbt Model Tests

## When this skill runs

Trigger this skill whenever the current turn created or modified any file under `models/**/*.sql`. Run it **before** calling `submit_summary` / the closing validation gate, so tests are in place when validation executes.

**Skip** a model if any of the following are true:
- Its `{{ config(...) }}` sets `materialized='ephemeral'`.
- Its `{{ config(...) }}` sets `enabled=false`.
- The `dbt_project.yml` disables it or marks it ephemeral for its path.

Do not prompt or write YAML for skipped models. Log a one-line reason and continue.

## Workflow (per changed model)

### 1. Gather context

For each changed `.sql` model:
- Resolve the model name from the file stem.
- Call `mcp__dbt_index__describe` with `node=<model_name>`, `columns=true` (no `sql`, no `lineage`) to get the column list.
- Check whether a `schema.yml` (or `schema.yaml` / `_models.yml`) co-located in the same directory already has an entry under `models:` with `name: <model_name>`. Read the existing entry if present.

### 2. Prompt the user (once, grouped, up-front)

Make a **single** `request_user_input` call per model with these three questions. Do not draft tests before asking.

- **Grain / primary key** — "What column(s) uniquely identify a row in `<model>`?" Provide 2–3 recommended options based on column names (e.g. an `id`-suffixed column). Include an "It's composite" option that will be followed up in the free-form Other field.
- **Additional non-null columns** — "Which columns must never be null?" Offer the most likely candidates as options (e.g. timestamp columns, foreign-key-looking columns). Users can select Other to list more.
- **Foreign keys** — "Any FK relationships to declare?" Offer the strongest candidates as options in the form `<col> -> ref('<model>').<col>` based on naming conventions (`customer_id`, `order_id`, etc.). Include an explicit "None" option.

If the model has more than ~8 columns, prefer surfacing only the top candidates and let Other capture the rest.

### 3. Apply defaults (no extra confirmation)

Given the answers, plan these tests automatically:
- **Single-column PK** → `not_null` and `unique` on that column.
- **Composite PK** → `not_null` on each part **plus** a model-level `dbt_utils.unique_combination_of_columns` test (see `references/test-patterns.md`). If `dbt_utils` is not in `packages.yml`, fall back to a `unique` test on a concatenation expression and note this in the summary.
- **Confirmed non-null columns** → `not_null` on each.
- **Confirmed FKs** → column-level `relationships` test with `to: ref('<parent>')` and `field: <parent_col>`.

Do **not** add `accepted_values`, freshness, or aggressive `not_null` on every `_id`/`_at` column. Those are opt-in only.

### 4. Merge into schema.yml (never overwrite)

Use `scripts/merge_schema_yml.py` to apply changes. The script:
- Preserves existing `description`, existing columns, and existing tests.
- Adds only tests that are not already present (set-equality on the test spec).
- Creates the file / model entry if missing.
- Writes only when there is a real diff.

If merging is impossible (malformed YAML, ambiguous location), stop and report the error. Do **not** overwrite silently.

### 5. Validate

Call `mcp__dbt_index__dbt_parse` (no selector needed) with a short `purpose` string. Fix any parse errors before ending the turn. Do **not** run `dbt test`; execution is the validation gate's job.

### 6. Report

Append one concise block per model to your final response (before summary):

```
tests added to <model>:
  - not_null, unique on id
  - relationships on customer_id -> ref('stg_customers').customer_id
  - not_null on order_ts
```

Skip the block for no-op models (idempotent re-runs).

## Script reference

See `scripts/merge_schema_yml.py` for the idempotent merge implementation. Invoke it with:

```
python3 scripts/merge_schema_yml.py \
  --schema-path <path/to/schema.yml> \
  --model-name <model> \
  --spec-json '<json spec>'
```

Where `<json spec>` is `{"columns": [{"name": "...", "tests": [...]}, ...], "model_tests": [...]}`. Tests are the standard dbt test dicts/strings (`"not_null"`, `"unique"`, `{"relationships": {"to": "ref('foo')", "field": "id"}}`, etc.).

## Test patterns

See `references/test-patterns.md` for the canonical YAML snippets to emit for single/composite PK, FK, and (opt-in) accepted_values.
