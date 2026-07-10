{{ config(materialized='table') }}

-- Chapter/CO/CHO/Fundraiser/Campaign mapping with data-quality validation
-- Flow: stg_google_sheet__master_mapping_sheet → int_google_sheet__chapter_mapping
-- Ports the legacy chapter_mapping_int validation rules onto the current PC/CRM models

with trimmed as (
    select * from {{ ref('stg_google_sheet__master_mapping_sheet') }}
),

-- Check 1: Duplicate composite key (chapter_id, cho_id) — both rows flagged
dup_chapter_cho as (
    select
        _airbyte_raw_id,
        'Duplicate (chapter_id, cho_id) — also in ' || (cnt - 1) || ' other row(s)' as issue_text
    from (
        select
            _airbyte_raw_id,
            count(*) over (partition by chapter_id, cho_id) as cnt
        from trimmed
    ) s
    where cnt > 1
),

-- Check 2: Duplicate fundraiser_id — both rows flagged
dup_fundraiser as (
    select
        _airbyte_raw_id,
        'Duplicate fundraiser_id ''' || fundraiser_id || ''' — also in ' || (cnt - 1) || ' other row(s)' as issue_text
    from (
        select
            _airbyte_raw_id,
            fundraiser_id,
            count(*) over (partition by fundraiser_id) as cnt
        from trimmed
        where fundraiser_id is not null
    ) s
    where cnt > 1
),

-- Check 3: Duplicate sourcing_campaign_code — both rows flagged
dup_campaign_code as (
    select
        _airbyte_raw_id,
        'Duplicate sourcing_campaign_code ''' || sourcing_campaign_code || ''' — also in ' || (cnt - 1) || ' other row(s)' as issue_text
    from (
        select
            _airbyte_raw_id,
            sourcing_campaign_code,
            count(*) over (partition by sourcing_campaign_code) as cnt
        from trimmed
        where sourcing_campaign_code is not null
    ) s
    where cnt > 1
),

-- Check 4a-d: required fields when chapter_status = 'Active'
active_missing_chapter_id as (
    select _airbyte_raw_id, 'Missing chapter_id (required for Active)' as issue_text
    from trimmed
    where chapter_status = 'Active' and chapter_id is null
),
active_missing_cho_id as (
    select _airbyte_raw_id, 'Missing cho_id (required for Active)' as issue_text
    from trimmed
    where chapter_status = 'Active' and cho_id is null
),
active_missing_worknode_id as (
    select _airbyte_raw_id, 'Missing worknode_id (required for Active)' as issue_text
    from trimmed
    where chapter_status = 'Active' and worknode_id is null
),
active_missing_fundraiser_id as (
    select _airbyte_raw_id, 'Missing fundraiser_id (required for Active)' as issue_text
    from trimmed
    where chapter_status = 'Active' and fundraiser_id is null
),

-- Check 5: engine must be E1 or E2
invalid_engine as (
    select
        _airbyte_raw_id,
        'Invalid engine ''' || coalesce(engine, 'NULL') || '''' as issue_text
    from trimmed
    where engine is null or engine not in ('E1', 'E2')
),

-- Check 6: chapter_status must be Active, Dropped out, or Pending status
invalid_chapter_status as (
    select
        _airbyte_raw_id,
        'Invalid chapter_status ''' || coalesce(chapter_status, 'NULL') || '''' as issue_text
    from trimmed
    where chapter_status is null
       or chapter_status not in ('Active', 'Dropped out', 'Pending status')
),

-- Check 7: fundraiser_id must be numeric if present
fundraiser_not_numeric as (
    select
        _airbyte_raw_id,
        'fundraiser_id ''' || fundraiser_id || ''' is not numeric' as issue_text
    from trimmed
    where fundraiser_id is not null
      and fundraiser_id ~ '[^0-9]'
),

-- Check 8: cho_id provided but not found in Platform Commons user data
invalid_cho_id as (
    select t._airbyte_raw_id, 'cho_id ''' || t.cho_id::text || ''' not found in user data' as issue_text
    from trimmed t
    where t.cho_id is not null
      and not exists (
          select 1 from {{ ref('int_pc_user_data') }} u where u."UserId"::numeric::integer = t.cho_id
      )
),

all_issues as (
    select _airbyte_raw_id, issue_text from dup_chapter_cho
    union all
    select _airbyte_raw_id, issue_text from dup_fundraiser
    union all
    select _airbyte_raw_id, issue_text from dup_campaign_code
    union all
    select _airbyte_raw_id, issue_text from active_missing_chapter_id
    union all
    select _airbyte_raw_id, issue_text from active_missing_cho_id
    union all
    select _airbyte_raw_id, issue_text from active_missing_worknode_id
    union all
    select _airbyte_raw_id, issue_text from active_missing_fundraiser_id
    union all
    select _airbyte_raw_id, issue_text from invalid_engine
    union all
    select _airbyte_raw_id, issue_text from invalid_chapter_status
    union all
    select _airbyte_raw_id, issue_text from fundraiser_not_numeric
    union all
    select _airbyte_raw_id, issue_text from invalid_cho_id
),

issue_agg as (
    select
        _airbyte_raw_id,
        string_agg(issue_text, '; ' order by issue_text) as validation_issues,
        count(*)::integer as issue_count
    from all_issues
    group by _airbyte_raw_id
),

-- chapter_id is free text on the sheet (e.g. "E1_Ban_04"); only cast it when it's
-- actually numeric so the E2 lookup below never trips over a non-numeric E1 id
chapter_id_lookup as (
    select
        _airbyte_raw_id,
        case when chapter_id ~ '^[0-9]+$' then chapter_id::integer else null end as chapter_id_int
    from trimmed
)

select
    t.city_name,
    t.state,
    t.co_id,
    t.sheet_co_name,
    co_user."UserDisplayName" as co_name,
    t.chapter_id,
    t.chapter_name,
    t.worknode_id,
    t.cho_id,
    t.sheet_cho_name,
    cho_user."UserDisplayName" as cho_name,
    t.engine,
    t.chapter_status,
    t.cho_status,
    t.sourcing_campaign_code,
    t.campaign_name,
    t.fundraiser_id,
    t.fundraiser_name,
    t.cho_allocation_on_pc,
    t.sheet_validation_status,
    t.sheet_validation_issues,
    case
        when t.engine = 'E2' and t.chapter_id is not null then partner_chapter.partner_id is not null
        else null
    end as chapter_validation,
    coalesce(i.issue_count, 0) = 0 as validation_status,
    i.validation_issues,
    coalesce(i.issue_count, 0) as issue_count,
    current_timestamp as validation_run_at
from trimmed t
left join {{ ref('int_pc_user_data') }} co_user
    on co_user."UserId"::numeric::integer = t.co_id
left join {{ ref('int_pc_user_data') }} cho_user
    on cho_user."UserId"::numeric::integer = t.cho_id
left join chapter_id_lookup cil
    on cil._airbyte_raw_id = t._airbyte_raw_id
left join {{ ref('int_crm__partners') }} partner_chapter
    on t.engine = 'E2'
    and cil.chapter_id_int = partner_chapter.partner_id
left join issue_agg i on t._airbyte_raw_id = i._airbyte_raw_id
