{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. Long form of prod_cci_session_breakdown for the two Sessions pies (spec
-- E1 §7 L5, the same columns as prod_do_session_groups_display): Dalgo pies take one metric, so each
-- planned-session group is a row. Same filter columns as prod_cci_session_breakdown_display, so the
-- District / DO / Chapter and Week / Month filters apply.
--   "What happened" (pie 1): Held / Cancelled / Volunteer absent / No volunteer assigned; the rows
--     for a CCI-week sum to its "Sessions planned".
--   "Held by" (pie 2, chart filter What happened = Held): Original volunteer / Substitute on Held
--     rows, so Held splits into two rows; empty on the other groups.
--   "Sessions": the count. Zero rows are dropped.
-- "Group order" sorts the groups for the legend (Held first). School's "Chapter organiser" column
-- has no CCI source, so it isn't here.
--
-- Grain: one row per CCI x week x "What happened" x "Held by", zero rows left out.

with base as (
    select * from {{ ref('prod_cci_session_breakdown') }}
),

long as (
    select b.*, g.what_happened, g.held_by, g.group_order, g.sessions
    from base b
    cross join lateral (
        values
            ('Held', 'Original volunteer', 1, b.sessions_held_by_original),
            ('Held', 'Substitute', 2, b.sessions_held_by_substitute),
            ('Cancelled', null, 3, b.sessions_cancelled),
            ('Volunteer absent', null, 4, b.sessions_volunteer_absent),
            ('No volunteer assigned', null, 5, b.sessions_no_volunteer_assigned)
    ) as g (what_happened, held_by, group_order, sessions)
)

select
    chapter_name        as "CCI",
    city                as "City",
    district            as "District",
    do_name             as "DO",
    week_label          as "Week",
    week_start          as "Week starting",
    month_label         as "Month",
    month_start         as "Month starting",
    what_happened       as "What happened",
    held_by             as "Held by",
    sessions            as "Sessions",
    group_order         as "Group order"
from long
where sessions > 0
