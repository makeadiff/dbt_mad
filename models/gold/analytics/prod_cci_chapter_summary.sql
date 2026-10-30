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
-- Started (§3.4): a circle with a session held since 1 June. Nothing here uses the weekly slot (§7
-- ruling 1) and there is no baseline (§3.7).
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
        coalesce(sum(sessions_held_since_1_oct), 0)::bigint as sessions_held_since_1_oct,
        count(*) filter (where has_weekly_slot) as circles_with_weekly_slot
    from {{ ref('prod_cci_mentoring_circles') }}
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
        coalesce(k.sessions_held_since_1_oct, 0) as sessions_held_since_1_oct,
        coalesce(k.circles_with_weekly_slot, 0) as circles_with_weekly_slot,
        coalesce(p.volunteers_placed, 0) as volunteers_placed,
        coalesce(st.circles_stopped_logging, 0) as circles_stopped_logging
    from chapters ch
    left join child_counts cc on ch.chapter_id = cc.chapter_id
    left join programme_counts pc on ch.chapter_id = pc.chapter_id
    left join sections s on ch.chapter_id = s.chapter_id
    left join circle_counts k on ch.chapter_id = k.chapter_id
    left join placed p on ch.chapter_id = p.chapter_id
    left join stopped st on ch.chapter_id = st.chapter_id
)

select
    b.*,
    b.active_children > 0 as has_children,
    round(100.0 * b.children_with_volunteer / nullif(b.active_children, 0), 1) as pct_children_with_volunteer,
    -- Volunteers needed (§3.3)
    ceil(b.active_children / 5.0)::int as volunteers_needed_min_1to5,
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
