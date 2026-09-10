{{ config(materialized='table') }}

-- dim_pc_school: one row per E1 (platform_commons) school, with name, active status, and
-- best-available city/state.
-- Grain: one row per school_id
-- Flow: int_pc_school_id (school_id, school_name, is_active) + stg_pc_worknode (CENTER, matched to
--       school_name by exact case-insensitive name -- there's no reliable FK between school and
--       worknode) + stg_pc_worknode_hierarchy (depth=1 ancestor = CITY, depth=2 ancestor = STATE)
--       -> dim_pc_school
--
-- city_name/state_name resolve for only 33 of 82 schools (2026-09) -- the worknode CENTER
-- name-match is the only linkage platform_commons offers between a school and its location, and
-- most schools simply have no CENTER worknode with a matching name. The same imperfect match is
-- already relied on elsewhere (int_pc_class_ops_master/int_pc_volunteer_attendance's
-- center_city_mapping). Null city_name/state_name here means the match failed, not that the school
-- has no real city/state.
-- Excludes CENTER worknodes literally named 'e2' (case-insensitive) -- a placeholder/junk value
-- that otherwise matches 19 different centers indiscriminately, which would have produced
-- confidently-wrong city/state for any school also (coincidentally) matched. After that exclusion,
-- every remaining school<->center name match is a clean 1:1 pairing (verified 2026-09-09: no school
-- matches more than one center and no center matches more than one school), so no further
-- deduplication/ambiguity handling is needed.

with school as (
    select * from {{ ref('int_pc_school_id') }}
),

center as (
    select * from {{ ref('stg_pc_worknode') }}
    where worknode_type = 'WN_TYPE.CENTER'
      and lower(trim(worknode_name)) <> 'e2'
),

hierarchy as (
    select * from {{ ref('stg_pc_worknode_hierarchy') }}
),

worknode as (
    select * from {{ ref('stg_pc_worknode') }}
),

school_center as (
    select
        s.school_id,
        c.worknode_id as center_id
    from school s
    join center c
        on lower(trim(c.worknode_name)) = lower(trim(s.school_name))
),

school_city as (
    select
        sc.school_id,
        w.worknode_name as city_name
    from school_center sc
    join hierarchy h on sc.center_id = h.worknode_id and h.depth = 1
    join worknode w on h.parent_worknode_id = w.worknode_id and w.worknode_type = 'WN_TYPE.CITY'
),

school_state as (
    select
        sc.school_id,
        w.worknode_name as state_name
    from school_center sc
    join hierarchy h on sc.center_id = h.worknode_id and h.depth = 2
    join worknode w on h.parent_worknode_id = w.worknode_id and w.worknode_type = 'WN_TYPE.STATE'
)

select
    s.school_id,
    s.school_name,
    sci.city_name,
    sst.state_name,
    s.is_active
from school s
left join school_city sci on s.school_id = sci.school_id
left join school_state sst on s.school_id = sst.school_id
