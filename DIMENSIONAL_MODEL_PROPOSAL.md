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
├── intermediate/                
│   ├── bubble/
│   ├── crm_data/
│   └── platform_commons/
├── marts/                       ← NEW: dimensional layer (tables)
│   └── core/
│       ├── dimensions/          (12 dim_ tables)
│       ├── facts/               (8 fct_ tables)
│       └── bridges/             (3 bridge_ tables)
```

See the metrics and then design the models and columns like that. 

  Staging (rename + cast only):
  - 7 existing models that are pure renames move directly here
  - All remaining source tables get new stg_ view models (1:1 with source)
  - Includes an example stg_bubble__children.sql model
  - Rules: no joins, no aggregations, no deduplication, materialized only

  Intermediate (reshape, join, deduplicate):
  - 10 dedup models (all CRM + platform user data using dbt_utils.deduplicate)
  - 17 join/enrichment models (ID lookups, foreign key resolution)
  - 11 complex logic models (CASE WHEN date parsing, business rules)
  - 2 new models proposed: int_applicants_unioned and int_partners_unified
  - fix
  - All 38 models listed with current name → new name mapping

  Marts (business-ready entities):
  - Rules for what belongs (dims, facts, bridges) and what doesn't
  - Materialized as tables, served to BI tools
  - Ties into the existing dimension/fact/bridge tables already in the proposal

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
| 3 | **fct_volunteer_slot_assignment** | One volunteer assigned to one slot-class-section | `slot_class_section_volunteer_int` + `slot_class_section_int` | volunteer_id, slot_id, class_section_id, assigned_date | is_active, is_removed |
| 4 | **fct_school_volunteer** | One volunteer assigned to one school | `school_volunteer_int` | school_id, volunteer_id, created_date | is_removed, academic_year |
| 7 | **fct_meetings** | One CRM meeting | `meetings_int` | partner_id, poc_id, user_id, meeting_date | follow_up_scheduled (boolean) |

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






