{{ config(materialized='table') }}

with volunteers as (
    select distinct
        volunteer_key,
        tagged_volunteer_id as volunteer_id
    from {{ ref('int_pc_volunteer_attendance') }}
)

select * from volunteers
