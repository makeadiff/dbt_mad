# dbt test patterns for `dbt-model-tests`

Canonical YAML snippets to emit. Use these exact shapes so the merge script's set-equality dedup works.

## Single-column primary key

```yaml
- name: my_model
  columns:
    - name: id
      tests:
        - not_null
        - unique
```

## Composite primary key (preferred: dbt_utils)

```yaml
- name: order_lines
  tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns:
          - order_id
          - line_number
  columns:
    - name: order_id
      tests: [not_null]
    - name: line_number
      tests: [not_null]
```

If `dbt_utils` is not installed (check `packages.yml`), fall back to:

```yaml
tests:
  - unique:
      column_name: "order_id || '-' || line_number"
```

and note the fallback in the summary so the user can add `dbt_utils` later.

## Foreign key (`relationships`)

```yaml
- name: customer_id
  tests:
    - not_null
    - relationships:
        to: ref('stg_customers')
        field: customer_id
```

Only add `not_null` on the FK column if the user marked it non-nullable.

## Non-null columns (user-confirmed)

```yaml
- name: created_at
  tests:
    - not_null
```

## Opt-in only (do NOT auto-apply)

These are shown for reference — apply only if the user explicitly asks or a follow-up prompt confirms them.

```yaml
- name: status
  tests:
    - accepted_values:
        values: ['pending', 'shipped', 'cancelled']
```

```yaml
- name: amount
  tests:
    - dbt_utils.expression_is_true:
        expression: ">= 0"
```

## Notes

- Use the short-form `not_null` and `unique` (bare strings) — the merge script normalizes these before comparing.
- Always attach `relationships` at the column level, not the model level.
- Prefer `ref('...')` targets; only use `source('...')` when the parent is truly a source.
