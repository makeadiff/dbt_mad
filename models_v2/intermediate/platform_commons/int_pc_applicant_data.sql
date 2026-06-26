{{ config(
    materialized='table'
) }}

with applicant as (
    select * from {{ ref('stg_pc_opportunity_applicant') }}
),

users as (
    select * from {{ ref('stg_pc_user') }}
),

profile as (
    select * from {{ ref('stg_pc_person_profile') }}
),

person as (
    select * from {{ ref('stg_pc_person') }}
),

meta as (
    select * from {{ ref('stg_pc_opportunity_applicant_meta') }}
),

person_address_bridge as (
    select * from {{ ref('stg_pc_person_person_addresses_bridge') }}
),

person_address as (
    select * from {{ ref('stg_pc_person_addresses') }}
),

address as (
    select * from {{ ref('stg_pc_address') }}
),

person_contact_bridge as (
    select * from {{ ref('stg_pc_person_person_contacts_bridge') }}
),

person_contact as (
    select * from {{ ref('stg_pc_person_contacts') }}
),

contact as (
    select * from {{ ref('stg_pc_contact') }}
),

worknode as (
    select * from {{ ref('stg_pc_worknode') }}
),

gender as (
    select * from {{ ref('stg_pc_gender') }}
),

-- De-duplicate addresses to pick only one per person
person_address_deduped as (
    select 
        pab.person_id,
        addr.city,
        addr.state,
        addr.country,
        addr.pincode,
        row_number() over (partition by pab.person_id order by addr.updated_datetime desc, addr.address_id desc) as rn
    from person_address_bridge pab
    join person_address pa on pab.person_address_id = pa.person_address_id
    join address addr on pa.address_id = addr.address_id
    where pa.is_active = true
),

-- De-duplicate contacts to pick only one primary mobile per person
person_contact_deduped as (
    select 
        pcb.person_id,
        c.contact_value,
        row_number() over (partition by pcb.person_id order by pc.is_primary desc, c.contact_id desc) as rn
    from person_contact_bridge pcb
    join person_contact pc on pcb.person_contact_id = pc.person_contact_id
    join contact c on pc.contact_id = c.contact_id
    where c.contact_type in ('CONTACT_TYPE.MOBILE', 'MOBILE')
)

select
    -- Surrogate Keys
    {{ dbt_utils.generate_surrogate_key(['a.opportunity_applicant_id']) }} as applicant_key,
    {{ dbt_utils.generate_surrogate_key(['a.user_id']) }} as user_key,
    {{ dbt_utils.generate_surrogate_key(['a.opportunity_id']) }} as opportunity_key,

    -- Identifier mapping to replicate Applicant_Data_2025
    a.opportunity_applicant_id as "ApplicationID",
    a.opportunity_id as "OpportunityId",
    a.user_id as "UserId",
    
    -- Status and Dates
    a.application_status as "ApplicationStatus",
    a.current_step_code as "CurrentStep",
    a.current_step_status as "CurrentStepStatus",
    a.application_datetime as "ApplicationDateTime",
    a.application_submit_datetime as "ApplicationSubmitDateTime",
    u.updated_datetime as "UserUpdatedDateTime",
    
    -- User Info
    u.login as "PrimaryEmailAddress",
    u.first_name as "FirstName",
    u.last_name as "LastName",
    u.first_name || ' ' || coalesce(u.last_name, '') as "DisplayName",
    coalesce(pc.contact_value, a.contact_number) as "MobileNumber",
    
    -- Profile Demographic
    p.date_of_birth as "DateOfBirth",
    upper(coalesce(nullif(p.gender_identifier, ''), g.gender_label)) as "Gender",
    p.occupation as "CurrentlyDoing",
    
    -- Location
    addr.city as "City",
    addr.state as "State",
    addr.country as "Country",
    addr.pincode as "Pincode",
    m.area_of_residence as "AreaOfResidence",
    
    -- Application Source
    coalesce(sourced_by_user.first_name || ' ' || coalesce(sourced_by_user.last_name, ''), a.applicant_referrer) as "Referrer",
    sourced_by_user.login as "ReferrerLogin",
    a.applicant_source as "ReferrerSource",
    a.applicant_medium as "ReferrerMedium",
    a.applicant_campaign as "ReferrerCampaign",
    sourced_by_user.user_id as "SourcedByUserId",
    
    -- Worknode Data Mapped
    w.worknode_name as "SelectedForWorkNodeName",
    w.worknode_type as "SelectedForWorkNodeType",
    
    -- Policies (from Meta)
    m.code_of_conduct_policy_accepted as "CodeOfConductPolicyAccepted",
    m.child_protection_policy_accepted as "ChildProtectionPolicyAccepted",
    
    current_timestamp as "_airbyte_extracted_at"

from applicant a
left join users u on a.user_id = u.user_id
left join person ps on u.person_id = ps.person_id
left join profile p on ps.person_profile_id = p.person_profile_id
left join gender g on p.gender_data_code = g.gender_data_code and g.rn = '1'
left join meta m on a.opportunity_applicant_id = m.opportunity_applicant_id

-- Address Join (joining to de-duplicating CTE)
left join person_address_deduped addr on u.person_id = addr.person_id and addr.rn = 1

-- Contact Join (joining to de-duplicating CTE)
left join person_contact_deduped pc on u.person_id = pc.person_id and pc.rn = 1

-- Worknode Join
left join worknode w on a.applied_to_entity_id = w.worknode_id

-- Sourced By / Referrer User Join
left join users sourced_by_user on 
    (case when a.applicant_referrer ~ '^[0-9]+$' then a.applicant_referrer::bigint else null end) = sourced_by_user.user_id

where a.is_deleted = false
