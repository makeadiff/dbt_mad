# Wizard skills (project-local)

Skills in this folder are auto-loaded by [dbt Wizard](https://github.com/dbt-labs/wizard)
whenever the CLI runs from within this repo. They ship with the project so every
contributor gets the same behavior.

## Skills bundled here

- **`dbt-model-tests`** — After any change to a `models/**/*.sql` file, Wizard
  prompts once for the model's grain/primary key, non-null columns, and FK
  relationships, then merges standard tests (`not_null`, `unique`,
  `relationships`, `dbt_utils.unique_combination_of_columns` for composite PKs)
  into the co-located `schema.yml`. Never overwrites existing entries.
  Ephemeral / disabled models are skipped. Validates with `dbt parse`.

## How to use

1. Install dbt Wizard (`curl -fsSL https://get.wizard.dbt.com | sh` or see the
   Wizard docs).
2. Run `wizard` from the repo root. The skills below will appear in the skill
   list automatically for the current project.
3. Edit or create a dbt model — the `dbt-model-tests` skill will trigger and
   prompt you for the info it needs before finishing the turn.

## Adding more skills

Drop another folder alongside `dbt-model-tests/` following the standard skill
layout:

```
skills/
└── my-skill/
    ├── SKILL.md          # required (name + description frontmatter)
    ├── agents/openai.yaml
    ├── references/
    └── scripts/
```

See `~/.dbt/wizard/skills/.system/skill-creator/SKILL.md` for the authoring
guide, or run Wizard with `$skill-creator` to scaffold one.
