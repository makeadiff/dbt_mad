{{ config(materialized='table') }}

with students as (
    select distinct
        student_key,
        "ChildId" as student_id,
        "ChildName" as student_name,
        "Gender" as gender,
        "ChildActiveStatus" as is_active
    from {{ ref('int_pc_child_attendance') }}
)

select * from students
