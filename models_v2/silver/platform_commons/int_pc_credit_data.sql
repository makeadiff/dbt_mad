{{ config(
    materialized='table'
) }}

with user_data as (
    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.updated_datetime,
        u.created_datetime,
        u.first_name || ' ' || coalesce(u.last_name, '') as volunteer_name
    from {{ ref('stg_pc_user') }} u
),

user_roles_agg as (
    select
        user_id,
        role_code as user_roles
    from {{ ref('stg_pc_user_role') }}
    where rn = 1
),

user_credit as (
    select * from {{ ref('stg_pc_user_credit_point') }}
),

credit_histories as (
    select * from {{ ref('stg_pc_credit_histories') }}
),

credit_bridge as (
    select * from {{ ref('stg_pc_userCreditPoint_creditHistories_bridge') }}
),

credit_agg as (
    select
        uc.user_id,
        max(uc.credit_point) as credit_point, -- Taking max as credit points for a user are grouped
        string_agg(ch.points::text, ',') as credit_point_history,
        string_agg(ch.reason, ',') as credit_point_history_reason
    from user_credit uc
    left join credit_bridge cb on uc.user_credit_point_id = cb.user_credit_point_id
    left join credit_histories ch on cb.credit_histories_id = ch.credit_history_id
    group by uc.user_id
),

opportunity_applicant as (
    select * from {{ ref('stg_pc_opportunity_applicant') }}
),

worknode as (
    select * from {{ ref('stg_pc_worknode') }}
),

worknode_mapping as (
    select
        a.user_id,
        -- Get unique centers
        string_agg(distinct w_center.worknode_name, ',') as tagged_at_name,
        string_agg(distinct w_center.worknode_type, ',') as tagged_at_type,
        -- Get unique cities
        string_agg(distinct w_city.worknode_name, ',') as parent_tagged_at_name,
        string_agg(distinct w_city.worknode_type, ',') as parent_tagged_at_type
    from opportunity_applicant a
    left join worknode w_center on a.applied_to_entity_id = w_center.worknode_id
    left join worknode w_city on a.secondary_applied_to_worknode_id = w_city.worknode_id
    where a.user_id is not null
    group by a.user_id
)

select
    {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as credit_key,
    {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as user_key,

    -- Columns required for parity with legacy API payload
    u.user_id as "UserId",
    r.user_roles as "UserRoles",
    coalesce(c.credit_point::numeric, 0) as "CreditPoint",
    w.tagged_at_name as "TaggedAtName",
    w.tagged_at_type as "TaggedAtType",
    
    u.created_datetime as "OnboardedDate",

    u.volunteer_name as "VolunteerName",
    c.credit_point_history as "CreditPointHistory",
    w.parent_tagged_at_name as "ParentTaggedAtName",
    w.parent_tagged_at_type as "ParentTaggedAtType",
    u.updated_datetime as "UserUpdatedDateTime",
    c.credit_point_history_reason as "CreditPointHistoryReason"

from user_data u
left join user_roles_agg r on u.user_id = r.user_id
left join credit_agg c on u.user_id = c.user_id
left join worknode_mapping w on u.user_id = w.user_id
where c.credit_point is not null
