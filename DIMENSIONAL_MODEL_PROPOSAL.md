# Dimensional Model Proposal for dbt_mad

## Part 1: What is a Kimball Star Schema?

### The Core Idea

A **Kimball star schema** is a way of organizing your data warehouse around **business processes** (things that happen) and **business context** (things you measure by). 

It has two types of tables:

| Table Type | Purpose | Example |
|---|---|---|
| **Fact table** | Records *events/transactions* — things that happened | A child attended a class, a donation was made |
| **Dimension table** | Describes the *who, what, where, when* context around events | The child's name/age, the school's city, the date |

The name "star schema" comes from the shape: a central fact table surrounded by dimension tables, forming a star.

### A Simple Example (from our domain)

Imagine we want to answer: *"How many children attended classes last month, broken down by school and volunteer?"*

**Without star schema** — you'd write a 100-line query joining 8 intermediate tables with CTEs, deduplication, and filters scattered everywhere (this is what `class_ops_data.sql` currently does).

**With star schema** — you'd write:

```sql
select
    p.partner_name   as school,
    u.user_display_name as volunteer,
    d.month_name,
    count(*) as attendance_count
from fct_child_attendance f
join dim_bubble_partner p on f.school_id = p.bubble_partner_id
join dim_user u            on f.slot_mentor_id = u.user_id
join dim_date d            on f.session_date_key = d.date_key
where d.month_name = 'January' and d.year = 2025
group by 1, 2, 3
```

Clean, readable, fast.

### Visual Structure

```
                    ┌──────────────┐
                    │  dim_child   │
                    │──────────────│
                    │ child_id (PK)│
                    │ first_name   │
                    │ gender       │
                    │ age          │
                    │ school_id    │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴───────────────┐   ┌──────────────┐
│  dim_school  │   │ fct_child_attendance  │   │   dim_date   │
│──────────────│   │──────────────────────│   │──────────────│
│ school_id(PK)├───┤ child_id (FK)        ├───┤ date_key (PK)│
│ school_name  │   │ school_id (FK)       │   │ date_day     │
│ city         │   │ session_date_key(FK) │   │ month_name   │
│ state        │   │ volunteer_id (FK)    │   │ quarter      │
│ school_type  │   │ attendance_status    │   │ academic_year│
└──────────────┘   │ did_participate      │   └──────────────┘
                    └──────┬───────────────┘
                           │
                    ┌──────┴───────┐
                    │  dim_user    │
                    │──────────────│
                    │ user_id (PK) │
                    │ display_name │
                    │ role         │
                    │ email        │
                    └──────────────┘
```

### Key Concepts

**Grain**: The most important decision — what does ONE ROW in a fact table represent? For `fct_child_attendance`, one row = one child in one session on one date. Every column in the fact must be true at that grain.

**Surrogate vs Natural Keys**: Natural keys come from source systems (`child_id = 1234`). Surrogate keys are generated integers. For this project, we'll use natural keys since we don't have SCD Type 2 requirements yet.

**Conformed Dimensions**: A dimension like `dim_user` is shared across multiple fact tables. The same `user_id` appears in `fct_child_attendance` (as the mentor), `fct_donations` (as the fundraiser), and `fct_school_volunteer` (as the volunteer). This means any query can slice any fact by user attributes consistently.

**Bridge Tables**: When you have many-to-many relationships (one child enrolled in multiple class sections, one class section has multiple children), you use a bridge table to sit between the fact and dimension.

### How It Differs From What We Have Today

| Aspect | Current (intermediate → prod) | Star Schema (staging → dims/facts) |
|---|---|---|
| Query complexity | 100-400 line CTEs in each analytics model | Simple joins between facts and dimensions |
| Reusability | Each prod model re-derives partner/user/child info | Dimensions are built once, referenced everywhere |
| Consistency | Different models may calculate "active children" differently | Single `dim_child` with `is_active` = one definition |
| Testing | Almost no tests | PK uniqueness, FK integrity, not-null on every table |
| Performance | Every model re-joins from intermediate | Facts pre-joined with FK integers, fast aggregation |
| New reports | Write a new 200-line model | Join existing facts + dims in a few lines |

---

## Part 2: Proposed Dimensional Model for dbt_mad

### New Folder Structure

```
models/
├── staging/                     ← NEW: 1:1 source mirrors (views)
│   ├── bubble/                  (22 models + source/model YAML)
│   ├── crm/                     (10 models + source/model YAML)
│   └── platform_commons/        (13 models + source/model YAML)
├── intermediate/                ← KEEP AS-IS (no changes)
│   ├── bubble/
│   ├── crm_data/
│   └── platform_commons/
├── marts/                       ← NEW: dimensional layer (tables)
│   └── core/
│       ├── dimensions/          (12 dim_ tables)
│       ├── facts/               (8 fct_ tables)
│       └── bridges/             (3 bridge_ tables)
├── prod/                        ← KEEP AS-IS (no changes)
│   ├── analytics/
│   └── external_apps/
```

### Dimensions (12 tables)

| # | Table | Grain (1 row = ) | Source | Key Columns |
|---|---|---|---|---|
| 1 | **dim_date** | One calendar day | Generated via `dbt_utils.date_spine` (2020–2030) | date_key, date_day, day_name, week_of_year, month_name, quarter, year, academic_year, is_weekend |
| 2 | **dim_crm_partner** | One CRM partner/school | `partners_int` + `cities_int` + `states_int` | crm_partner_id (PK), partner_name, city, state, pincode, school_type, lead_source, is_removed |
| 3 | **dim_bubble_partner** | One Bubble partner/school | `partner_int` | bubble_partner_uuid (PK), bubble_partner_id (int), partner_name, city, state, co_name, poc_name, school_type, mou_dates, child_counts |
| 4 | **dim_user** | One user (volunteer/CO/staff) | `user_data_int` (deduplicated) | user_id (PK), display_name, email, contact, city, role, reporting_manager_id |
| 5 | **dim_child** | One child | `child_int` | child_id (PK), first_name, last_name, gender, dob, age, is_active, school_id, enrollment_date |
| 6 | **dim_class_section** | One class section | `class_section_int` + `school_class_int` + `class_int` | class_section_id (PK), section_name, class_name, school_id, academic_year, is_active |
| 7 | **dim_subject** | One subject | `subject_int` | subject_id (PK), subject_name, program_id |
| 8 | **dim_program** | One program | `program_int` | program_id (PK), program_name |
| 9 | **dim_slot** | One recurring time slot | `slot_int` | slot_id (PK), slot_name, day_of_week, start_time, end_time, duration_minutes, school_id, academic_year |
| 10 | **dim_chapter** | One chapter | `chapter_int` | chapter_id (PK), chapter_name, city, state, academic_year |
| 11 | **dim_campaign** | One fundraising campaign | Derived from `fundraising_donations_int` | campaign_id (PK), campaign_name, payment_campaign, program_name |
| 12 | **dim_mou** | One MOU agreement | `mous_int` | mou_id (PK), partner_id (FK), mou_status, sign_date, start_date, end_date, confirmed_child_count |

### Fact Tables (8 tables)

| # | Table | Grain (1 row = ) | Source | Dimension FKs | Measures |
|---|---|---|---|---|---|
| 1 | **fct_child_attendance** | One child, one session | `child_attendance_int` | child_id, school_id, section_id, mentor_id, session_date | attendance_status, did_participate, did_understand, did_complete_task |
| 2 | **fct_donations** | One donation transaction | `fundraising_donations_int` | fundraiser_id, campaign_id, payment_date | donation_amount, tip_amount, total_amount_paid, donation_type |
| 3 | **fct_volunteer_slot_assignment** | One volunteer assigned to one slot-class-section | `slot_class_section_volunteer_int` + `slot_class_section_int` | volunteer_id, slot_id, class_section_id, assigned_date | is_active, is_removed |
| 4 | **fct_school_volunteer** | One volunteer assigned to one school | `school_volunteer_int` | school_id, volunteer_id, created_date | is_removed, academic_year |
| 5 | **fct_applicant** | One application | `applicant_data_2023/2024/2025_int` + `fellow_applicant_data_int` (UNION) | user_id, application_date | application_status, current_step, sourced_medium, sourced_source |
| 6 | **fct_events** | One volunteer attending one event | `events_data_int` | volunteer_id, attendance_marked_date | attendance_status, event_type |
| 7 | **fct_meetings** | One CRM meeting | `meetings_int` | partner_id, poc_id, user_id, meeting_date | follow_up_scheduled (boolean) |
| 8 | **fct_credits** | One credit transaction | `credit_data_int` | user_id, onboarded_date | credit_point, credit_point_history |

### Bridge Tables (3 tables)

| # | Table | Purpose | Source | Keys |
|---|---|---|---|---|
| 1 | **bridge_child_class_section** | Child ↔ Class Section (many-to-many) | `child_class_section_int` | child_id, class_section_id, academic_year |
| 2 | **bridge_partner_co** | Partner ↔ CO assignment history | `partner_cos_int` | partner_id, co_id, created_at, updated_at |
| 3 | **bridge_child_subject** | Child ↔ Subject enrollment | `child_subject_int` | child_id, class_section_subject_id, academic_year |

### Tests to Add

For every table:
- `unique` + `not_null` on primary key
- `not_null` on all foreign keys
- `relationships` test (FK exists in parent dimension)
- `accepted_values` on status/type columns (e.g., attendance_status, application_status)
- `dbt_expectations.expect_column_values_to_be_between` on monetary amounts

---

## Part 3: What Changes in dbt_project.yml

```yaml
models:
  dbt_mad:
    staging:
      +materialized: view          # lightweight, no storage cost
    marts:
      core:
        +materialized: table       # persisted for query performance
```

The `macros/generate_schema_name.sql` may need a small update to route `staging` and `marts` models to appropriate schemas.

---

## Part 4: What Stays Untouched

- All 47 intermediate models — no changes
- All 12 production models — no changes
- `models/schema.yml` — no changes
- `packages.yml` — no changes (dbt_utils already has date_spine)
- `macros/` — minor update only if schema routing is needed

---

## Part 5: Manual Effort Estimate

### Phase Breakdown

| Phase | What | # of Files | Estimated Time | Notes |
|---|---|---|---|---|
| **Phase 1: Staging** | 45 `stg_` view models + 6 YAML files | ~51 files | **3–4 days** | Repetitive but straightforward. Each model is ~15-30 lines of column renaming. Source YAMLs need freshness configs migrated. |
| **Phase 2: Dimensions** | 12 `dim_` table models + model YAML | ~13 files | **3–4 days** | Moderate complexity. dim_date needs date_spine setup. dim_crm_partner and dim_class_section require multi-table joins. Others are simpler. |
| **Phase 3: Facts** | 8 `fct_` table models + model YAML | ~9 files | **4–5 days** | Most complex phase. fct_child_attendance and fct_volunteer_slot_assignment need careful grain definition. fct_applicant requires UNION of 4 sources with column alignment. |
| **Phase 4: Bridges** | 3 `bridge_` table models | ~3 files | **0.5 day** | Simple pass-through of existing junction tables with proper FK columns. |
| **Phase 5: Tests & Docs** | YAML with tests + column descriptions | ~4 files | **2–3 days** | Writing unique/not_null/relationships tests for every PK/FK across 23 new tables. Column-level descriptions for all dimensions. |
| **Phase 6: Validation** | Run, test, compare row counts | — | **1–2 days** | dbt run, dbt test, spot-check counts, validate FK integrity, ensure prod models still work. |

### Total Estimate

| Scenario | Time |
|---|---|
| **One person, focused** | **~2.5 to 3 weeks** |
| **One person, alongside other work** | **~4 to 5 weeks** |
| **Two people in parallel** (one on staging+dims, one on facts+bridges) | **~1.5 to 2 weeks** |

### Effort Distribution

```
Staging (30%) ████████████░░░░░░░░░░░░░░░░░░  repetitive, low risk
Dimensions   (20%) ████████░░░░░░░░░░░░░░░░░░░░░░  moderate, some joins
Facts        (25%) ██████████░░░░░░░░░░░░░░░░░░░░  highest complexity
Bridges      (3%)  █░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  trivial
Tests/Docs   (12%) █████░░░░░░░░░░░░░░░░░░░░░░░░░  tedious but important
Validation   (10%) ████░░░░░░░░░░░░░░░░░░░░░░░░░░  critical for confidence
```

### Risk Factors That Could Add Time

- **Partner ID reconciliation**: CRM and Bubble partner IDs don't have a clean mapping table. Fact tables referencing schools will need to be careful about which partner dimension they FK to. May need investigation time.
- **Applicant data UNION**: The 3 yearly applicant tables have different column sets (2023 is very different from 2024/2025). Aligning them into one `fct_applicant` will require careful column mapping.
- **Academic year logic**: dim_date needs an `academic_year` column. Need to confirm the exact month boundaries (likely June–May for Indian academic year).
- **Existing prod models**: While we're not changing them, we need to verify they still build correctly after adding staging + marts.

### What You Get For This Investment

1. **Any new analytics report** becomes a 10-20 line query instead of 200+ lines
2. **Consistent definitions** — "active child", "recruited volunteer" defined once
3. **Test coverage** — catch data issues before they hit dashboards
4. **Self-documenting** — new team members can understand the data model from table names alone
5. **Foundation for incremental models** — fct_child_attendance can later become incremental for performance
6. **BI tool friendly** — star schemas are what Metabase/Looker/Superset expect
