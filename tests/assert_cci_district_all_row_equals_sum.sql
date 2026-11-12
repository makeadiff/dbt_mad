-- The "All districts" row equals the sum of the district rows, and of the CCIs, on every layout v4
-- count (spec E1 §7 L1-L7; the older counts are in assert_cci_district_summary_equals_chapters).
-- The national display reads the same All districts row. Returns a row only when something differs.
with districts as (
    select
        sum(ccis) as ccis,
        sum(ccis_setup_complete) as ccis_setup_complete,
        sum(children_in_circle_no_slot) as children_in_circle_no_slot,
        sum(weekly_session_slots) as weekly_session_slots,
        sum(circles_no_weekly_slot) as circles_no_weekly_slot,
        sum(volunteers_minimum_to_start) as volunteers_minimum_to_start,
        sum(volunteers_needed_as_set_up_1to5) as volunteers_needed,
        sum(leads_chapter_link) as leads_chapter_link,
        sum(sessions_planned_since_1_oct) as planned,
        sum(sessions_held_since_1_oct) as held,
        sum(sessions_cancelled_since_1_oct) as cancelled,
        sum(sessions_volunteer_absent_since_1_oct) as absent,
        sum(sessions_no_volunteer_assigned_since_1_oct) as no_volunteer
    from {{ ref('prod_cci_district_summary') }}
    where district <> 'All districts'
),

chapters as (
    select
        count(*) filter (where has_children) as ccis,
        count(*) filter (where setup_complete) as ccis_setup_complete,
        sum(children_in_circle_no_slot) as children_in_circle_no_slot,
        sum(weekly_session_slots) as weekly_session_slots,
        sum(circles_no_weekly_slot) as circles_no_weekly_slot,
        sum(volunteers_minimum_to_start) as volunteers_minimum_to_start,
        sum(volunteers_needed_as_set_up_1to5) as volunteers_needed,
        sum(leads_chapter_link) as leads_chapter_link,
        sum(sessions_planned_since_1_oct) as planned,
        sum(sessions_held_since_1_oct) as held,
        sum(sessions_cancelled_since_1_oct) as cancelled,
        sum(sessions_volunteer_absent_since_1_oct) as absent,
        sum(sessions_no_volunteer_assigned_since_1_oct) as no_volunteer
    from {{ ref('prod_cci_chapter_summary') }}
),

all_row as (
    select
        ccis, ccis_setup_complete, children_in_circle_no_slot, weekly_session_slots, circles_no_weekly_slot,
        volunteers_minimum_to_start, volunteers_needed_as_set_up_1to5 as volunteers_needed, leads_chapter_link,
        sessions_planned_since_1_oct as planned, sessions_held_since_1_oct as held,
        sessions_cancelled_since_1_oct as cancelled, sessions_volunteer_absent_since_1_oct as absent,
        sessions_no_volunteer_assigned_since_1_oct as no_volunteer
    from {{ ref('prod_cci_district_summary') }}
    where district = 'All districts'
)

select 'All districts <> districts' as problem
from all_row a, districts d
where row(a.ccis::numeric, a.ccis_setup_complete::numeric, a.children_in_circle_no_slot::numeric, a.weekly_session_slots::numeric, a.circles_no_weekly_slot::numeric, a.volunteers_minimum_to_start::numeric, a.volunteers_needed::numeric, a.leads_chapter_link::numeric, a.planned::numeric, a.held::numeric, a.cancelled::numeric, a.absent::numeric, a.no_volunteer::numeric)
   is distinct from row(d.ccis::numeric, d.ccis_setup_complete::numeric, d.children_in_circle_no_slot::numeric, d.weekly_session_slots::numeric, d.circles_no_weekly_slot::numeric, d.volunteers_minimum_to_start::numeric, d.volunteers_needed::numeric, d.leads_chapter_link::numeric, d.planned::numeric, d.held::numeric, d.cancelled::numeric, d.absent::numeric, d.no_volunteer::numeric)
union all
select 'All districts <> CCIs'
from all_row a, chapters c
where row(a.ccis::numeric, a.ccis_setup_complete::numeric, a.children_in_circle_no_slot::numeric, a.weekly_session_slots::numeric, a.circles_no_weekly_slot::numeric, a.volunteers_minimum_to_start::numeric, a.volunteers_needed::numeric, a.leads_chapter_link::numeric, a.planned::numeric, a.held::numeric, a.cancelled::numeric, a.absent::numeric, a.no_volunteer::numeric)
   is distinct from row(c.ccis::numeric, c.ccis_setup_complete::numeric, c.children_in_circle_no_slot::numeric, c.weekly_session_slots::numeric, c.circles_no_weekly_slot::numeric, c.volunteers_minimum_to_start::numeric, c.volunteers_needed::numeric, c.leads_chapter_link::numeric, c.planned::numeric, c.held::numeric, c.cancelled::numeric, c.absent::numeric, c.no_volunteer::numeric)
union all
select 'missing All districts row' where not exists (select 1 from all_row)
union all
select 'more than one All districts row' where (select count(*) from all_row) > 1
