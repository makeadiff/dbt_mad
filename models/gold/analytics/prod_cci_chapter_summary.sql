{{ config(materialized='table') }}

-- CCI dashboard (E1) metric model, snake_case. One row per CCI (chapter) for AY 2026-27, from the
-- seed's 28 schools, so every CCI appears even with no children (Sarada Niketan, Brodipet). Reads
-- prod_cci_active_children and prod_cci_mentoring_circles; the Dalgo-facing names are in
-- prod_cci_chapters_display. Rules: spec E1 §3.0-§3.4.
--
-- District and DO: from seed_cci_chapter_group (spec E1 §7 D1, the DO sheet's Chapter mapping tab).
-- Child status, worst first (§3.2): "not yet in a mentoring circle" (one of their sections has no
-- circle) -> "in a circle with no volunteer" (a circle on their sections has none) -> with a volunteer.
-- Volunteer requirement met (§3.1): at least 1 circle, every circle has a volunteer, no section with
-- children is without a circle. 2:5 additionally needs 2+ volunteers on every circle.
-- Volunteers needed: minimum = children / 5 rounded up per CCI; as set up = 1 per circle + 1 per
-- section with no circle. 2:5 is children x 2 / 5 rounded up, and double the as-set-up figure.
-- Volunteers placed: distinct volunteers on the CCI's circles; the national figure sums the CCIs.
-- Stopped logging (§3.11): a circle with a session held before 1 Oct and none since, from
-- prod_cci_session_dates. Needing a volunteer (§3.11): circles with no volunteer + sections with no circle.
-- Started (§3.4): a circle with a session held since 1 June. No baseline (§3.7).
--
-- Layout v4 (spec §7 L1-L7, copying DO_DASHBOARD_SPEC.md §3.8, 2026-11-12):
-- Weekly slot: a circle's slot day from Platform Commons' class ops data (prod_cci_mentoring_circles).
-- Weekly session slots = distinct slot (day + name) across the CCI's circles. Child status gains
-- School's "in a mentoring circle with no weekly slot", worst first: not yet in a circle -> in a
-- circle with no weekly slot -> in a circle with no volunteer -> with a volunteer; the four sum to
-- active children, so "with a volunteer" now also needs every circle to have a slot day.
-- Setup complete: children with data, none outside a circle, none in a circle with no slot.
-- Minimum volunteers to start = ceil(children / 5) (the old "needed, minimum 1:5"). Volunteers
-- needed stays as set up (L2). Volunteers mapped to a chapter (L3) stays off: no year-scoped source.
-- Sessions since 1 Oct (planned, held, by substitute, cancelled, volunteer absent, no volunteer
-- assigned) from prod_cci_planned_sessions, so held now counts planned dates only (a session logged
-- on a circle with no slot day isn't in it).
-- Leads (L4): this year's applicants who applied and aren't hired (fct_volunteer_pipeline is_applied
-- and not is_recruited_new). leads_chapter_link = those whose 2026-27 application's campaign is the
-- CCI's sourcing_campaign_code on the chapter mapping sheet (engine E1, chapter_id = school_id; codes
-- btrimmed of tabs, two carry a leading tab). fct_volunteer_pipeline only joins E2 codes, so it files
-- these applicants as 'city' or 'chapter_unmatched'; this model leaves it unchanged (no shared-fact
-- edits) and resolves the E1 codes here. leads_city = 'city' leads in the CCI's city, less the ones
-- with a CCI code (they count on that CCI), repeated on every CCI in the city: never summed. PC city
-- Gurugram serves Delhi, as on School. The national open pool is on prod_cci_national_display.
--
-- Grain: one row per chapter_id.

with chapters as (
    select
        school_id::text as chapter_id,
        chapter_name,
        city,
        district,
        do_name
    from {{ ref('seed_cci_chapter_group') }}
),

section_circles as (
    select
        chapter_id,
        sc_level_batch_id,
        count(*) as circles,
        count(*) filter (where not has_weekly_slot) as circles_no_slot,
        count(*) filter (where volunteers = 0) as circles_no_volunteer
    from {{ ref('prod_cci_mentoring_circles') }}
    group by 1, 2
),

child_sections as (
    select
        ac.chapter_id,
        ac.student_id,
        ac.sc_level_batch_id,
        coalesce(sc.circles, 0) = 0 as section_has_no_circle,
        coalesce(sc.circles_no_slot, 0) > 0 as section_has_circle_no_slot,
        coalesce(sc.circles_no_volunteer, 0) > 0 as section_has_circle_no_volunteer
    from {{ ref('prod_cci_active_children') }} ac
    left join section_circles sc
        on ac.chapter_id = sc.chapter_id
       and ac.sc_level_batch_id = sc.sc_level_batch_id
),

children as (
    select
        chapter_id,
        student_id,
        case
            when bool_or(section_has_no_circle) then 'not_in_circle'
            when bool_or(section_has_circle_no_slot) then 'circle_no_slot'
            when bool_or(section_has_circle_no_volunteer) then 'circle_no_volunteer'
            else 'with_volunteer'
        end as child_status
    from child_sections
    group by 1, 2
),

child_counts as (
    select
        chapter_id,
        count(*) as active_children,
        count(*) filter (where child_status = 'with_volunteer') as children_with_volunteer,
        count(*) filter (where child_status = 'not_in_circle') as children_not_in_circle,
        count(*) filter (where child_status = 'circle_no_slot') as children_in_circle_no_slot,
        count(*) filter (where child_status = 'circle_no_volunteer') as children_in_circle_no_volunteer
    from children
    group by 1
),

programme_counts as (
    select
        chapter_id,
        count(distinct student_id) filter (where programme = 'Foundations') as children_foundations,
        count(distinct student_id) filter (where programme = 'Education Support') as children_education_support,
        count(distinct student_id) filter (where programme = 'Transition Readiness') as children_transition_readiness,
        count(distinct student_id) filter (where programme = 'Programme not set') as children_programme_not_set
    from {{ ref('prod_cci_active_children') }}
    group by 1
),

sections as (
    select
        s.chapter_id,
        count(distinct s.sc_level_batch_id) as sections_with_children,
        count(distinct s.sc_level_batch_id) filter (where coalesce(sc.circles, 0) = 0) as sections_no_circle
    from {{ ref('prod_cci_active_children') }} s
    left join section_circles sc
        on s.chapter_id = sc.chapter_id
       and s.sc_level_batch_id = sc.sc_level_batch_id
    group by 1
),

circle_counts as (
    select
        chapter_id,
        count(*) as mentoring_circles,
        count(*) filter (where volunteers = 0) as circles_no_volunteer,
        count(*) filter (where volunteers = 1) as circles_one_volunteer,
        count(*) filter (where volunteers >= 2) as circles_two_plus_volunteers,
        count(*) filter (where is_started) as circles_started,
        count(*) filter (where has_weekly_slot) as circles_with_weekly_slot,
        count(distinct slot_day || '|' || coalesce(slot_name, '')) filter (where has_weekly_slot) as weekly_session_slots
    from {{ ref('prod_cci_mentoring_circles') }}
    group by 1
),

planned as (
    select
        chapter_id,
        count(*) as sessions_planned_since_1_oct,
        count(*) filter (where session_group in ('held_by_original', 'held_by_substitute')) as sessions_held_since_1_oct,
        count(*) filter (where session_group = 'held_by_substitute') as sessions_held_by_substitute_since_1_oct,
        count(*) filter (where session_group = 'cancelled') as sessions_cancelled_since_1_oct,
        count(*) filter (where session_group = 'volunteer_absent') as sessions_volunteer_absent_since_1_oct,
        count(*) filter (where session_group = 'no_volunteer_assigned') as sessions_no_volunteer_assigned_since_1_oct
    from {{ ref('prod_cci_planned_sessions') }}
    group by 1
),

leads as (
    select volunteer_id, lead_attribution, lower(trim(city)) as pc_city
    from {{ ref('fct_volunteer_pipeline') }}
    where is_applied and not is_recruited_new
),

link_applicants as (
    select volunteer_id, chapter_id from {{ ref('prod_cci_link_applicants') }}
),

leads_by_chapter as (
    select la.chapter_id, count(distinct l.volunteer_id) as leads_chapter_link
    from leads l
    join link_applicants la on l.volunteer_id = la.volunteer_id
    group by 1
),

leads_by_city as (
    select
        case l.pc_city when 'gurugram' then 'delhi' else l.pc_city end as city_key,
        count(distinct l.volunteer_id) as leads_city
    from leads l
    where l.lead_attribution = 'city'
      and not exists (select 1 from link_applicants la where la.volunteer_id = l.volunteer_id)
    group by 1
),

stopped as (
    select
        c.chapter_id,
        count(*) as circles_stopped_logging
    from {{ ref('prod_cci_mentoring_circles') }} c
    where exists (
            select 1 from {{ ref('prod_cci_session_dates') }} d
            where d.worknode_slot_shift_id = c.worknode_slot_shift_id
              and d.session_date < date '2026-10-01')
      and not exists (
            select 1 from {{ ref('prod_cci_session_dates') }} d
            where d.worknode_slot_shift_id = c.worknode_slot_shift_id
              and d.session_date >= date '2026-10-01')
    group by 1
),

placed as (
    select chapter_id, count(distinct u) as volunteers_placed
    from {{ ref('prod_cci_mentoring_circles') }}
    cross join lateral unnest(volunteer_user_ids) as u
    group by 1
),

base as (
    select
        ch.chapter_id,
        ch.chapter_name,
        ch.city,
        ch.district,
        ch.do_name,
        coalesce(cc.active_children, 0) as active_children,
        coalesce(cc.children_with_volunteer, 0) as children_with_volunteer,
        coalesce(cc.children_not_in_circle, 0) as children_not_in_circle,
        coalesce(cc.children_in_circle_no_slot, 0) as children_in_circle_no_slot,
        coalesce(cc.children_in_circle_no_volunteer, 0) as children_in_circle_no_volunteer,
        coalesce(pc.children_foundations, 0) as children_foundations,
        coalesce(pc.children_education_support, 0) as children_education_support,
        coalesce(pc.children_transition_readiness, 0) as children_transition_readiness,
        coalesce(pc.children_programme_not_set, 0) as children_programme_not_set,
        coalesce(s.sections_with_children, 0) as sections_with_children,
        coalesce(s.sections_no_circle, 0) as sections_no_circle,
        coalesce(k.mentoring_circles, 0) as mentoring_circles,
        coalesce(k.circles_no_volunteer, 0) as circles_no_volunteer,
        coalesce(k.circles_one_volunteer, 0) as circles_one_volunteer,
        coalesce(k.circles_two_plus_volunteers, 0) as circles_two_plus_volunteers,
        coalesce(k.circles_started, 0) as circles_started,
        coalesce(k.circles_with_weekly_slot, 0) as circles_with_weekly_slot,
        coalesce(k.weekly_session_slots, 0) as weekly_session_slots,
        coalesce(pl.sessions_planned_since_1_oct, 0) as sessions_planned_since_1_oct,
        coalesce(pl.sessions_held_since_1_oct, 0) as sessions_held_since_1_oct,
        coalesce(pl.sessions_held_by_substitute_since_1_oct, 0) as sessions_held_by_substitute_since_1_oct,
        coalesce(pl.sessions_cancelled_since_1_oct, 0) as sessions_cancelled_since_1_oct,
        coalesce(pl.sessions_volunteer_absent_since_1_oct, 0) as sessions_volunteer_absent_since_1_oct,
        coalesce(pl.sessions_no_volunteer_assigned_since_1_oct, 0) as sessions_no_volunteer_assigned_since_1_oct,
        coalesce(lc.leads_chapter_link, 0) as leads_chapter_link,
        coalesce(lcity.leads_city, 0) as leads_city,
        coalesce(p.volunteers_placed, 0) as volunteers_placed,
        coalesce(st.circles_stopped_logging, 0) as circles_stopped_logging
    from chapters ch
    left join child_counts cc on ch.chapter_id = cc.chapter_id
    left join programme_counts pc on ch.chapter_id = pc.chapter_id
    left join sections s on ch.chapter_id = s.chapter_id
    left join circle_counts k on ch.chapter_id = k.chapter_id
    left join placed p on ch.chapter_id = p.chapter_id
    left join stopped st on ch.chapter_id = st.chapter_id
    left join planned pl on ch.chapter_id = pl.chapter_id
    left join leads_by_chapter lc on ch.chapter_id = lc.chapter_id
    left join leads_by_city lcity on lower(trim(ch.city)) = lcity.city_key
)

select
    b.*,
    b.active_children > 0 as has_children,
    round(100.0 * b.children_with_volunteer / nullif(b.active_children, 0), 1) as pct_children_with_volunteer,
    -- Volunteers needed (§3.3)
    ceil(b.active_children / 5.0)::int as volunteers_needed_min_1to5,
    ceil(b.active_children / 5.0)::int as volunteers_minimum_to_start,
    b.mentoring_circles - b.circles_with_weekly_slot as circles_no_weekly_slot,
    (b.active_children > 0 and b.children_not_in_circle = 0 and b.children_in_circle_no_slot = 0) as setup_complete,
    round(100.0 * (b.sessions_held_since_1_oct - b.sessions_held_by_substitute_since_1_oct)
        / nullif(b.sessions_planned_since_1_oct, 0), 1) as pct_held_by_original_since_1_oct,
    round(100.0 * b.sessions_held_since_1_oct / nullif(b.sessions_planned_since_1_oct, 0), 1) as pct_sessions_held_since_1_oct,
    round(100.0 * b.sessions_cancelled_since_1_oct / nullif(b.sessions_planned_since_1_oct, 0), 1) as pct_sessions_cancelled_since_1_oct,
    ceil(b.active_children * 2 / 5.0)::int as volunteers_needed_min_2to5,
    b.mentoring_circles + b.sections_no_circle as volunteers_needed_as_set_up_1to5,
    b.circles_no_volunteer + b.sections_no_circle as circles_needing_volunteer,
    b.mentoring_circles - b.circles_started as circles_not_started,
    2 * (b.mentoring_circles + b.sections_no_circle) as volunteers_needed_as_set_up_2to5,
    -- Metric 1: volunteer requirement met (§3.1)
    (b.active_children > 0 and b.mentoring_circles >= 1 and b.circles_no_volunteer = 0 and b.sections_no_circle = 0)
        as volunteer_requirement_met_1to5,
    (b.active_children > 0 and b.mentoring_circles >= 1 and b.circles_no_volunteer = 0 and b.sections_no_circle = 0
        and b.circles_one_volunteer = 0) as volunteer_requirement_met_2to5,
    case
        when b.active_children = 0 then 'No children this year'
        when b.mentoring_circles = 0 then 'No mentoring circles set up'
        when b.sections_no_circle > 0 then 'Some sections have no mentoring circle'
        when b.circles_no_volunteer > 0 then 'Some mentoring circles have no volunteer'
    end as volunteer_requirement_not_met_reason,
    -- Metric 2: sessions commenced (§3.4)
    (b.circles_started >= 1) as has_sessions_started,
    (b.mentoring_circles >= 1 and b.circles_started = b.mentoring_circles) as all_circles_started,
    round(100.0 * b.circles_started / nullif(b.mentoring_circles, 0), 1) as pct_circles_started,
    case
        when b.active_children = 0 then 'No children this year'
        when b.mentoring_circles = 0 then 'No mentoring circles set up'
        when b.circles_started < b.mentoring_circles then 'Some mentoring circles haven''t started'
    end as not_all_circles_started_reason
from base b
