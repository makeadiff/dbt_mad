Session-Ops Backend — Database Schema

Django 4.2 + PostgreSQL. 27 models across sessionops/models/.

Conventions (apply to almost every model unless noted)

- Soft delete only — no model ever issues SQL DELETE. Deactivation sets is_active=False (and often a legacy removed=True flag) plus deleted_at. A delete() override enforces this; hard_delete() always raises.
- Standard audit columns on most domain models: is_active (bool, default True), removed (bool, default False — legacy duplicate of is_active on older models), deleted_at (nullable), created_at (auto now add), updated_at (auto now), created_by / updated_by (FK → User, on_delete=PROTECT, usually nullable).
- All foreign keys use on_delete=PROTECT unless explicitly noted otherwise (only 2 exceptions in the whole schema: PasswordResetToken.user uses CASCADE, and Partner.co_id/similar Hasura-sourced IDs are plain integers with no DB-level FK at all).
- Primary keys are BigAutoField (bigint, auto-increment) named <model>_id, except User.user_id, SyncRun.id.
- Default manager on soft-deletable models filters is_active=True; use .all_with_deleted() to bypass.

Legend: FK→X = ForeignKey to model X. uniq = part of a unique constraint. idx = indexed.

---

1. Identity & Auth

User (users)

The custom user model — never use Django's built-in User.

┌──────────────────────────────┬───────────────┬───────┬──────────────────────────────────────────────────────────────────────┐
│            Field             │     Type      │ Null? │                                Notes                                 │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_id                      │ bigint PK     │ —     │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_display_name            │ varchar(255)  │ no    │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_login                   │ varchar(255)  │ no    │ unique, idx — usually the email                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ email                        │ varchar(255)  │ no    │ idx, not unique (Hasura source has dupes)                            │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ contact                      │ varchar(20)   │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ is_active                    │ bool          │ no    │ default True — auth gate, distinct from soft-delete                  │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_role                    │ text          │ no    │ comma-separated roles synced from Hasura, e.g. "CO Part              │
│                              │               │       │ Time,Wingman" — parsed at runtime                                    │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ reporting_manager_user_login │ varchar(255)  │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ reporting_manager_role_code  │ varchar(50)   │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ reporting_manager_user_id    │ bigint        │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ added_by                     │ varchar(255)  │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ city / state / center        │ varchar(100)  │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_created_datetime        │ timestamp     │ no    │ auto_now_add                                                         │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user_updated_datetime        │ timestamp     │ no    │ auto_now                                                             │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ last_login_at                │ timestamp     │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ synced_at                    │ timestamp     │ yes   │ last Hasura refresh                                                  │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ worknode_id                  │ int           │ yes   │ idx — links user to a school via PartnerWorknode                     │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ deleted_at                   │ timestamp     │ yes   │                                                                      │
├──────────────────────────────┼───────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ deleted_by                   │ FK→User       │ yes   │ implements soft-delete directly (can't inherit base —                │
│                              │ (self)        │       │ self-referential FK)                                                 │
└──────────────────────────────┴───────────────┴───────┴──────────────────────────────────────────────────────────────────────┘

Indexes: email, user_login, user_role, (state, city).
Properties/helpers: .id (alias for user_id), .full_name, .is_manager (role contains "manager"/"admin"), .get_auth_methods(), .has_password_auth(), .has_google_auth().

UserAuth (user_auth)

One row per (user, auth method). Inherits standard soft-delete base.

┌───────────────────────┬──────────────┬───────┬───────────────────────────────────────────────────────────────────────────────┐
│         Field         │     Type     │ Null? │                                     Notes                                     │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ user                  │ FK→User      │ no    │ related_name auth_methods                                                     │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ auth_type             │ varchar(16)  │ no    │ choices: google, password                                                     │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ auth_identifier       │ varchar(255) │ no    │ lookup key — google_sub for google rows, lowercased user_login for password   │
│                       │              │       │ rows                                                                          │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ password_hash         │ varchar(255) │ yes   │ argon2id hash, only for password type                                         │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ google_email_verified │ bool         │ yes   │ only for google type                                                          │
├───────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────────────────────────────────┤
│ last_used_at          │ timestamp    │ yes   │                                                                               │
└───────────────────────┴──────────────┴───────┴───────────────────────────────────────────────────────────────────────────────┘

Constraints:
- uniq_active_auth_lookup: unique (auth_type, auth_identifier) where is_active=True
- uniq_active_auth_per_user_per_type: unique (user, auth_type) where is_active=True — one active method of each type per user
- auth_type_field_integrity (CHECK): google rows must have password_hash NULL; password rows must have it set

PasswordResetToken (password_reset_tokens)

┌─────────────────────┬─────────────┬───────┬──────────────────────────────────────────────────────────────────────┐
│        Field        │    Type     │ Null? │                                Notes                                 │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ user                │ FK→User     │ no    │ on_delete=CASCADE (the one exception to PROTECT)                     │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ token               │ UUID        │ no    │ unique, idx, default uuid4()                                         │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ created_at          │ timestamp   │ no    │                                                                      │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ expires_at          │ timestamp   │ no    │ auto-set to created_at + 30 min if not given                         │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ used_at             │ timestamp   │ yes   │ set when consumed or superseded                                      │
├─────────────────────┼─────────────┼───────┼──────────────────────────────────────────────────────────────────────┤
│ invalidation_reason │ varchar(20) │ yes   │ choices: consumed, superseded; null = still active/expired naturally │
└─────────────────────┴─────────────┴───────┴──────────────────────────────────────────────────────────────────────┘

No hard rows ever deleted — kept for audit. Computed .status property: valid / consumed / superseded / time_expired.

---

2. School / Partner (synced from Hasura CRM)

Partner (partner)

"Partner" = Hasura/CRM term; frontend calls these schools. Inherits SoftDeleteBaseModel.

┌──────────────────────────────────────────────────┬──────────────┬───────┬───────────────────────────────────────────────────┐
│                      Field                       │     Type     │ Null? │                       Notes                       │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ partner_id                                       │ bigint       │ no    │ unique, idx — the Hasura ID (not the Django PK)   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ partner_name                                     │ varchar(255) │ no    │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ co_id                                            │ bigint       │ yes   │ idx — no DB FK (Hasura ID may outpace local User  │
│                                                  │              │       │ sync)                                             │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ co_name                                          │ varchar(255) │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ address_line_1/2                                 │ text         │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ city, city_id, state, state_id, pincode          │ mixed        │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ school_type                                      │ varchar(100) │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ partner_affiliation_type                         │ varchar(100) │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ poc_name / poc_email / poc_designation /         │ varchar      │ yes   │ point of contact                                  │
│ poc_contact                                      │              │       │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ mou_sign_date / mou_start_date / mou_end_date    │ date         │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ mou_url                                          │ text         │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ converted                                        │ bool         │ no    │ default False                                     │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ crm_partner_removed                              │ bool         │ no    │ default False                                     │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ latest_conversion_stage                          │ varchar(50)  │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ lead_source                                      │ varchar(100) │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ date_of_first_contact                            │ timestamp    │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ confirmed_child_count / total_child_count        │ int          │ yes   │ denormalized from Hasura                          │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ classes                                          │ text         │ yes   │                                                   │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ partner_created_date / partner_updated_date      │ timestamp    │ yes   │ from Hasura                                       │
├──────────────────────────────────────────────────┼──────────────┼───────┼───────────────────────────────────────────────────┤
│ synced_at                                        │ timestamp    │ yes   │ our sync time                                     │
└──────────────────────────────────────────────────┴──────────────┴───────┴───────────────────────────────────────────────────┘

Has an unfiltered all_objects manager (for sync upserts to find soft-deleted rows). Indexes on co_id, partner_updated_date.

PartnerWorknode (partner_worknode)

Sync mirror of Hasura's chapter_mapping table. Read-only from Session-Ops; hard-delete IS allowed here (it's a sync mirror, not domain data).

┌───────────────────────────────────────────────────────────────────────┬──────────────┬───────┬───────┐
│                                 Field                                 │     Type     │ Null? │ Notes │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ partner_worknode_id                                                   │ bigint PK    │ —     │       │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ partner_id                                                            │ varchar(100) │ no    │ idx   │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ worknode_id                                                           │ int          │ no    │ idx   │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ city_name, state, chapter_name, engine, chapter_status, campaign_name │ varchar      │ yes   │       │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ co_name, sourcing_campaign_code, fundraiser_name                      │ text         │ yes   │       │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ fundraiser_id                                                         │ varchar(100) │ yes   │       │
├───────────────────────────────────────────────────────────────────────┼──────────────┼───────┼───────┤
│ created_at / updated_at                                               │ timestamp    │ no    │       │
└───────────────────────────────────────────────────────────────────────┴──────────────┴───────┴───────┘

---

3. Academic Structure

AcademicYear (academic_year)

┌──────────────────┬─────────────┬───────┬──────────────────────────┐
│      Field       │    Type     │ Null? │          Notes           │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ academic_year_id │ bigint PK   │ —     │                          │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ label            │ varchar(20) │ no    │ unique, e.g. "2026-2027" │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ is_active        │ bool        │ no    │ default False            │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ removed          │ bool        │ no    │                          │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ created_by       │ FK→User     │ no    │                          │
├──────────────────┼─────────────┼───────┼──────────────────────────┤
│ updated_by       │ FK→User     │ yes   │                          │
└──────────────────┴─────────────┴───────┴──────────────────────────┘

Constraint: uniq_active_academic_year — only one row can have is_active=True globally.

SchoolAcademicYear (school_academic_year)

┌─────────────────────────┬─────────────────┬───────┬───────────────────────────────────────┐
│          Field          │      Type       │ Null? │                 Notes                 │
├─────────────────────────┼─────────────────┼───────┼───────────────────────────────────────┤
│ school_academic_year_id │ bigint PK       │ —     │                                       │
├─────────────────────────┼─────────────────┼───────┼───────────────────────────────────────┤
│ school_id               │ bigint          │ no    │ idx — loose ref to Partner.partner_id │
├─────────────────────────┼─────────────────┼───────┼───────────────────────────────────────┤
│ academic_year_id        │ FK→AcademicYear │ no    │                                       │
├─────────────────────────┼─────────────────┼───────┼───────────────────────────────────────┤
│ is_active, removed      │ bool            │ no    │                                       │
├─────────────────────────┼─────────────────┼───────┼───────────────────────────────────────┤
│ created_by / updated_by │ FK→User         │ mixed │                                       │
└─────────────────────────┴─────────────────┴───────┴───────────────────────────────────────┘

Constraint: unique (school_id, academic_year_id) where removed=False.

Program (program)

┌────────────────────┬──────────────┬───────┬────────┐
│       Field        │     Type     │ Null? │ Notes  │
├────────────────────┼──────────────┼───────┼────────┤
│ program_id         │ bigint PK    │ —     │        │
├────────────────────┼──────────────┼───────┼────────┤
│ program_name       │ varchar(100) │ no    │ unique │
├────────────────────┼──────────────┼───────┼────────┤
│ is_active, removed │ bool         │ no    │        │
└────────────────────┴──────────────┴───────┴────────┘

Class (class) — universal catalog, no lifecycle

┌────────────┬─────────────┬───────┬──────────────────┐
│   Field    │    Type     │ Null? │      Notes       │
├────────────┼─────────────┼───────┼──────────────────┤
│ class_id   │ bigint PK   │ —     │                  │
├────────────┼─────────────┼───────┼──────────────────┤
│ class_name │ varchar(20) │ no    │ e.g. "5th"       │
├────────────┼─────────────┼───────┼──────────────────┤
│ class_code │ varchar(4)  │ no    │ unique, e.g. "5" │
├────────────┼─────────────┼───────┼──────────────────┤
│ program_id │ FK→Program  │ no    │                  │
└────────────┴─────────────┴───────┴──────────────────┘

SchoolClass (school_class)

┌─────────────────────────┬───────────────────────┬───────┬───────┐
│          Field          │         Type          │ Null? │ Notes │
├─────────────────────────┼───────────────────────┼───────┼───────┤
│ school_class_id         │ bigint PK             │ —     │       │
├─────────────────────────┼───────────────────────┼───────┼───────┤
│ school_id               │ bigint                │ no    │ idx   │
├─────────────────────────┼───────────────────────┼───────┼───────┤
│ school_academic_year_id │ FK→SchoolAcademicYear │ no    │       │
├─────────────────────────┼───────────────────────┼───────┼───────┤
│ class_id                │ FK→Class              │ no    │       │
├─────────────────────────┼───────────────────────┼───────┼───────┤
│ is_active, removed      │ bool                  │ no    │       │
└─────────────────────────┴───────────────────────┴───────┴───────┘

Constraint: unique (school_id, school_academic_year_id, class_id) where removed=False.

ClassSection (class_section)

A section within a SchoolClass (e.g. "5th - A"), or a normalized Bucket slug (M6 pivot).

┌──────────────────────┬────────────────┬───────┬──────────────────────────┐
│        Field         │      Type      │ Null? │          Notes           │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ class_section_id     │ bigint PK      │ —     │                          │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ school_class_id      │ FK→SchoolClass │ yes   │                          │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ school_id            │ bigint         │ no    │ idx                      │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ section_code         │ char(1)        │ yes   │ choices A–L              │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ section_name         │ varchar(100)   │ no    │ "5th - A" or bucket slug │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ section_display_name │ varchar(255)   │ yes   │                          │
├──────────────────────┼────────────────┼───────┼──────────────────────────┤
│ is_active, removed   │ bool           │ no    │                          │
└──────────────────────┴────────────────┴───────┴──────────────────────────┘

Constraints: unique (school_class_id, section_code) where removed=False AND section_code not null; unique (school_id, section_name) where is_active=True AND removed=False.

Subject (subject) — universal catalog, no lifecycle

┌──────────────┬──────────────┬───────┬─────────────────────┐
│    Field     │     Type     │ Null? │        Notes        │
├──────────────┼──────────────┼───────┼─────────────────────┤
│ subject_id   │ bigint PK    │ —     │                     │
├──────────────┼──────────────┼───────┼─────────────────────┤
│ subject_name │ varchar(100) │ no    │ unique              │
├──────────────┼──────────────┼───────┼─────────────────────┤
│ program_id   │ FK→Program   │ no    │ db_constraint=False │
└──────────────┴──────────────┴───────┴─────────────────────┘

ClassSectionSubject (class_section_subject)

┌──────────────────────────┬─────────────────┬───────┬────────────────────────────────────────────┐
│          Field           │      Type       │ Null? │                   Notes                    │
├──────────────────────────┼─────────────────┼───────┼────────────────────────────────────────────┤
│ class_section_subject_id │ bigint PK       │ —     │                                            │
├──────────────────────────┼─────────────────┼───────┼────────────────────────────────────────────┤
│ class_section_id         │ FK→ClassSection │ no    │                                            │
├──────────────────────────┼─────────────────┼───────┼────────────────────────────────────────────┤
│ subject_id               │ FK→Subject      │ no    │                                            │
├──────────────────────────┼─────────────────┼───────┼────────────────────────────────────────────┤
│ is_active, removed       │ bool            │ no    │ idx (class_section_id, is_active, removed) │
└──────────────────────────┴─────────────────┴───────┴────────────────────────────────────────────┘

---

4. Children

Child (child)

┌────────────────────────┬──────────────┬───────┬─────────────────────────────────────┐
│         Field          │     Type     │ Null? │                Notes                │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ child_id               │ bigint PK    │ —     │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ school_id              │ bigint       │ no    │ idx                                 │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ first_name / last_name │ varchar(100) │ no    │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ gender                 │ varchar(20)  │ no    │ choices: male, female, other        │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ date_of_birth          │ date         │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ age                    │ int          │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ city                   │ varchar(100) │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ mother_tongue          │ varchar(50)  │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ date_of_enrollment     │ date         │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ mad_joining_date       │ date         │ yes   │                                     │
├────────────────────────┼──────────────┼───────┼─────────────────────────────────────┤
│ is_active, removed     │ bool         │ no    │ idx (school_id, is_active, removed) │
└────────────────────────┴──────────────┴───────┴─────────────────────────────────────┘

ChildClass (child_class) — child ↔ SchoolClass link

child_id FK→Child, school_class_id FK→SchoolClass, is_active/removed, idx on both FKs + is_active.

ChildClassSection (child_class_section) — child ↔ ClassSection link

child_id FK→Child, class_section_id FK→ClassSection, is_active/removed, idx on both FKs + is_active.

BatchChild (batch_child) — child ↔ SchoolAcademicYear cohort link

child_id FK→Child, school_academic_year_id FK→SchoolAcademicYear, school_id (bigint, idx), is_active/removed.

ChildProgram (child_program) — child ↔ Program link

child_id FK→Child, program_id FK→Program, is_active/removed.

ChildRemovalLog (child_removal_log)

Audit trail for why/when a child was deactivated — mandatory reason.

┌──────────────────┬─────────────┬───────┬─────────────────────────────────────────────────────────────────────────────────────┐
│      Field       │    Type     │ Null? │                                        Notes                                        │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ child_id         │ FK→Child    │ no    │                                                                                     │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ co_id            │ bigint      │ no    │ loose FK to user_id (no DB constraint)                                              │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ school_id        │ bigint      │ no    │ idx                                                                                 │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ removed_reason   │ varchar(20) │ no    │ choices: inactive, duplicate_entry, wrong_school_class, transferred, dropped_out,   │
│                  │             │       │ family_declined, child_declined, other                                              │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ other_details    │ text        │ yes   │                                                                                     │
├──────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────────────────────────────────┤
│ removed_datetime │ timestamp   │ no    │                                                                                     │
└──────────────────┴─────────────┴───────┴─────────────────────────────────────────────────────────────────────────────────────┘

ChildSubject (child_subject)

History-only — never used for current-state queries. child_id FK→Child, class_section_subject_id FK→ClassSectionSubject, is_active/removed.

---

5. Scheduling / Volunteers

Slot (slot)

Weekly recurring time window at a school.

┌─────────────────────────┬───────────────────────┬───────┬───────────────────────────────────────────────────────────────┐
│          Field          │         Type          │ Null? │                             Notes                             │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ slot_id                 │ bigint PK             │ —     │                                                               │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ school_id               │ bigint                │ no    │ idx                                                           │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ school_academic_year_id │ FK→SchoolAcademicYear │ no    │                                                               │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ slot_name               │ varchar(100)          │ no    │                                                               │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ day_of_week             │ varchar(10)           │ no    │ choices: monday…sunday                                        │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ start_time / end_time   │ time                  │ no    │                                                               │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ recurring               │ bool                  │ no    │ default True                                                  │
├─────────────────────────┼───────────────────────┼───────┼───────────────────────────────────────────────────────────────┤
│ is_active, removed      │ bool                  │ no    │ idx (school_id, is_active, removed), (school_id, day_of_week) │
└─────────────────────────┴───────────────────────┴───────┴───────────────────────────────────────────────────────────────┘

SlotClassSection (slot_class_section)

slot_id FK→Slot, class_section_id FK→ClassSection, class_section_subject_id FK→ClassSectionSubject (denormalized), is_active/removed.

SlotClassSectionVolunteer (slot_class_section_volunteer)

slot_class_section_id FK→SlotClassSection, volunteer_id FK→User, is_active/removed. One row per volunteer; each slot-class has 1–2 rows (R2).

SchoolVolunteer (school_volunteer)

De-dup record: one volunteer per school (R4). school_id (bigint, idx), volunteer_id FK→User, school_academic_year_id FK→SchoolAcademicYear (nullable), is_active/removed.

SchoolSessionDetails (school_session_details)

session_id PK, school_id (bigint, idx), school_academic_year FK→SchoolAcademicYear, start_date/end_date (date), is_active/removed. Immutable once created.

SchoolHoliday (school_holiday)

┌───────────────────────────────┬─────────────┬───────┬─────────────────────────────────────────────────────────┐
│             Field             │    Type     │ Null? │                          Notes                          │
├───────────────────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────┤
│ school_holiday_id             │ bigint PK   │ —     │                                                         │
├───────────────────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────┤
│ school_id                     │ bigint      │ no    │ idx                                                     │
├───────────────────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────┤
│ holiday_reason                │ varchar(50) │ no    │ choices: mad_event, holidays, cancelled_from_school_end │
├───────────────────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────┤
│ start_date / end_date         │ date        │ no    │ single-day = start==end                                 │
├───────────────────────────────┼─────────────┼───────┼─────────────────────────────────────────────────────────┤
│ holiday_description / remarks │ text        │ yes   │                                                         │
└───────────────────────────────┴─────────────┴───────┴─────────────────────────────────────────────────────────┘

---

6. Sync / Audit (append-only, insert-only)

SyncRun (sync_run)

Does not inherit SoftDeleteBaseModel — immutable append-only trail.

┌───────────────────────────────────────────────────────┬──────────────┬───────┬──────────────────────────────────────────────┐
│                         Field                         │     Type     │ Null? │                    Notes                     │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ id                                                    │ bigint PK    │ —     │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ started_at                                            │ timestamp    │ no    │ auto_now_add, idx                            │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ completed_at                                          │ timestamp    │ yes   │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ status                                                │ varchar(16)  │ no    │ choices: running, success, failed            │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ entity_sync_type                                      │ varchar(20)  │ no    │ legacy — choices: users, partners,           │
│                                                       │              │       │ partner_worknode, all                        │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ run_type                                              │ varchar(30)  │ yes   │ choices: auto (cron), manual,                │
│                                                       │              │       │ manual_single_user                           │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ entity_type                                           │ varchar(30)  │ yes   │ choices: user, partner, partner_worknode     │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ updated_after / cursor_end                            │ timestamp    │ yes   │ cursor_end = max Hasura updated_at seen      │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ target_identifier                                     │ varchar(200) │ yes   │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ triggered_by                                          │ FK→User      │ yes   │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ user_logins / partner_ids                             │ JSON         │ yes   │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ users_fetched/created/updated,                        │ int          │ no    │ default 0                                    │
│ partners_fetched/created/updated                      │              │       │                                              │
├───────────────────────────────────────────────────────┼──────────────┼───────┼──────────────────────────────────────────────┤
│ error_message                                         │ text         │ yes   │                                              │
└───────────────────────────────────────────────────────┴──────────────┴───────┴──────────────────────────────────────────────┘

RealtimeSyncLog (realtime_sync_log)

Append-only audit log for M8a webhook/n8n realtime sync events.

┌─────────────────────────────────────────┬──────────────┬───────┬────────────────────────────────────────────────────────────┐
│                  Field                  │     Type     │ Null? │                           Notes                            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ realtime_sync_log_id                    │ bigint PK    │ —     │                                                            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ user_id_from_source                     │ int          │ no    │ idx — not a DB FK                                          │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ sync_type                               │ varchar(30)  │ no    │ choices: manual_admin, realtime_webhook, cron_fallback     │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ event_type                              │ varchar(20)  │ no    │ choices: insert, update, deactivate                        │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ triggered_by                            │ FK→User      │ yes   │                                                            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ external_event_id                       │ varchar(200) │ yes   │ idx                                                        │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ received_at                             │ timestamp    │ no    │ auto_now_add, idx                                          │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ processed_at                            │ timestamp    │ yes   │                                                            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ status                                  │ varchar(30)  │ no    │ choices: success, partial_success, failed,                 │
│                                         │              │       │ skipped_role_not_allowed, skipped_no_change, skipped_stale │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│                                         │              │       │ choices: user_created, common_fields_updated,              │
│ action_taken                            │ varchar(40)  │ no    │ worknode_added/updated/removed, user_deactivated,          │
│                                         │              │       │ no_change, no_school_found_for_worknode, failed            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ error_details                           │ text         │ yes   │                                                            │
├─────────────────────────────────────────┼──────────────┼───────┼────────────────────────────────────────────────────────────┤
│ pre_snapshot / incoming_payload /       │              │       │                                                            │
│ field_changes / cascaded_changes /      │ JSON         │ yes   │                                                            │
│ rules_fired / deferred_operations       │              │       │                                                            │
└─────────────────────────────────────────┴──────────────┴───────┴────────────────────────────────────────────────────────────┘

---

Quick relationship map

User ──< UserAuth (auth methods)
User ──< PasswordResetToken
User ──< SyncRun.triggered_by, RealtimeSyncLog.triggered_by
User ──< SchoolVolunteer, SlotClassSectionVolunteer (as volunteer)
User ──< *.created_by / *.updated_by / *.deleted_by (audit trail, almost every table)

Partner (school, Hasura-synced) ──loose ref (school_id, no FK)── most domain tables
Partner.co_id ──loose ref (no FK)── User (CO assignment)
PartnerWorknode ──loose ref (partner_id/worknode_id, no FK)── Partner / User.worknode_id

AcademicYear ──< SchoolAcademicYear ──< SchoolClass, Slot, SchoolVolunteer, BatchChild
Program ──< Class, Subject
Class ──< SchoolClass ──< ClassSection ──< ClassSectionSubject
ClassSection ──< ChildClassSection, SlotClassSection
ClassSectionSubject ──< ChildSubject (history), SlotClassSection

Child ──< ChildClass, ChildClassSection, ChildProgram, BatchChild, ChildRemovalLog, ChildSubject

Slot ──< SlotClassSection ──< SlotClassSectionVolunteer

✻ Baked for 3m 25s
                                                                                              ✔ Update installed · Restart to apply
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
❯
──────────────────────────────────────────────────────────────────────────────────────