{{ config(materialized='table') }}

-- Resolves school_name for each PC school (bridge -> schoolName)
-- Flow: stg_pc_school + stg_pc_school_schoolName_bridge + stg_pc_school_name -> int_pc_school_id
-- Verified: every school has exactly one name row total (no dupes to rank), so the
-- row_number() here is defensive, not covering a known conflict today.
-- language_code is either 'ENG' or NULL in the source (no other languages exist) -
-- filtering strictly on 'ENG' (as other PC models do) silently drops 28 of 84 schools
-- whose name row just wasn't tagged with a language code, so both are accepted here.

with school as (
    select * from {{ ref('stg_pc_school') }}
),

school_name_bridge as (
    select
        *,
        row_number() over (partition by school_id order by school_name_id desc) as rn
    from {{ ref('stg_pc_school_schoolName_bridge') }}
),

school_name as (
    select * from {{ ref('stg_pc_school_name') }}
)

select
    s.school_id,
    sn.center_name as school_name,
    s.is_active,
    s.updated_datetime as user_updated_date_time
from school s
left join school_name_bridge snb on s.school_id = snb.school_id and snb.rn = 1
left join school_name sn on snb.school_name_id = sn.school_name_id
    and (sn.language_code = 'ENG' or sn.language_code is null)
