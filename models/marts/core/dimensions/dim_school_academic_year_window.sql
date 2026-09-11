{{ config(materialized='table') }}

-- dim_school_academic_year_window: the planned-session date window for one school's academic year
-- Grain: one row per (school_id, academic_year)
-- Feeds fct_e2_sessions_summary's total_planned_sessions calc for the E2 sessionops dashboard only --
-- volunteer/child consistency (fct_e2_volunteer_consistency / fct_e2_child_consistency) keep the
-- existing per-allocation "planned after the volunteer got assigned into a slot" logic untouched.
-- Each academic_year label (e.g. '2025-2026') is given an inferred calendar span of
-- [April 1 of the first year, March 30 of the second year] purely to test date overlap below --
-- academic_year itself carries no stored date range, only a text label.
-- window_start_date/window_end_date come from one of two sources, flagged by window_source:
--   'session_detail' -- int_bubble__school_session_detail has a row for this school_academic_year_id
--     (start_date/end_date straight from Bubble; latest is_active/non-removed row wins if more than
--     one ever exists). Rare in practice: confirmed 2026-09-11 that only 11 schools have any
--     session_detail row at all, against 191 school+academic_year combos overall.
--   'mou_default' -- no session_detail row, so falls back to int_crm__mous. int_crm__mous.partner_id
--     is the same id space as Bubble's school_id (confirmed via prod_volunteer_allocation_history's
--     `cs.school_id = p.crm_partner_id` join) -- no bridge table needed. An MOU is matched to this
--     academic_year when its [mou_sign_date, mou_end_date] window overlaps the AY's inferred
--     calendar span -- NOT by year(mou_end_date) alone, since a meaningful share of MOUs run
--     multi-year (confirmed terms up to 5 years, e.g. mou_end_date in 2029/2030 for schools signed
--     in 2025), and an exact-year match was silently dropping those to 'no_data' despite a perfectly
--     valid MOU being on file. When more than one MOU overlaps the same AY (confirmed: duplicate
--     MOUs signed the same day, plus an old multi-year MOU overlapping a newer one at renewal), the
--     ranking prefers -- in order -- (1) any candidate whose resulting window wouldn't invert
--     (see below), (2) the MOU whose own sign date falls INSIDE this AY's calendar span (it
--     "originates" this AY) over one from an earlier/later year passing through, (3) latest
--     mou_sign_date, (4) highest mou_id. Rule (1) exists because an originating MOU signed very
--     close to this AY's own end date can push mou_sign_date + 60 days past window_end_date --
--     confirmed real cases where that inverted candidate would otherwise have outranked a perfectly
--     good older pass-through MOU for the same AY, wrongly producing 'no_data'.
--     window_start_date = mou_sign_date + 60 days ONLY for the AY the matched MOU actually
--     originates in; for a later AY still covered by that same multi-year MOU, window_start_date is
--     that AY's own inferred calendar start (April 1) instead -- using the original sign date + 60
--     days there would predate the AY by years and blow up the week count.
--     window_end_date is always a hardcoded March 30 of the AY's end year, never mou_end_date
--     itself -- mou_end_date is unreliable in the source data (confirmed null on some rows, and
--     wildly inconsistent term lengths on others).
--   'no_data' -- neither a session_detail row nor an overlapping MOU exists for this school+AY, or
--     every overlapping MOU candidate still resolves to an inverted window (start after end).
--     Confirmed 2026-09-11: with the ranking above, 0 of 191 school+AY rows currently fall here --
--     kept as a safety net so a future school with genuinely no MOU/session_detail data at all still
--     gets a row (with null dates) instead of breaking downstream joins.
-- academic_year is exposed as a label (not just school_academic_year_id) for the same reason
-- dim_school_academic_year_status does: it's resolved once here via the school_academic_year_id ->
-- academic_year_id -> label FK chain, so consumers can join on (school_id, academic_year) without
-- re-walking that chain themselves.

with school_academic_years as (
    -- One row per (school_id, academic_year_id), carrying the authoritative school_academic_year_id
    -- PK. Same "latest PK per business key" dedup as dim_school_academic_year_status, but that dim
    -- doesn't expose school_academic_year_id itself, which int_bubble__school_session_detail needs
    -- to join on.
    select distinct on (say.school_id, say.academic_year_id)
        say.school_academic_year_id,
        say.school_id,
        say.academic_year_id,
        ay.label as academic_year,
        right(ay.label, 4)::int as ay_end_year,
        make_date(right(ay.label, 4)::int - 1, 4, 1) as ay_calendar_start,
        make_date(right(ay.label, 4)::int, 3, 30) as ay_calendar_end
    from {{ ref('int_bubble__school_academic_year') }} say
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    order by say.school_id, say.academic_year_id, say.modified_date desc, say.created_date desc
),

session_detail_window as (
    -- Latest active, non-removed session_detail row per school_academic_year_id (only matters if
    -- more than one is ever recorded for the same AY; today it's always exactly one).
    select distinct on (school_academic_year_id)
        school_academic_year_id,
        start_date as session_detail_start_date,
        end_date as session_detail_end_date
    from {{ ref('int_bubble__school_session_detail') }}
    where is_removed = false
        and is_active = true
    order by school_academic_year_id, modified_date desc
),

mou_overlap as (
    select
        m.partner_id as school_id,
        m.mou_id,
        m.mou_sign_date,
        say.school_academic_year_id,
        say.ay_calendar_start,
        (m.mou_sign_date between say.ay_calendar_start and say.ay_calendar_end) as originates_this_ay,
        -- A non-originating (pass-through) candidate always produces a valid window: its
        -- window_start_date falls back to this AY's own calendar start, which is always <=
        -- ay_calendar_end. An originating candidate only produces a valid window if
        -- mou_sign_date + 60 days doesn't push past this AY's own end date. Rank on that
        -- uniformly first, so a perfectly good older pass-through MOU never loses to a newer
        -- originating one that would actually invert.
        (
            case
                when (m.mou_sign_date between say.ay_calendar_start and say.ay_calendar_end)
                    then (m.mou_sign_date + interval '60 days')::date <= say.ay_calendar_end
                else true
            end
        ) as produces_valid_window,
        row_number() over (
            partition by m.partner_id, say.school_academic_year_id
            order by
                (
                    case
                        when (m.mou_sign_date between say.ay_calendar_start and say.ay_calendar_end)
                            then (m.mou_sign_date + interval '60 days')::date <= say.ay_calendar_end
                        else true
                    end
                ) desc,
                (m.mou_sign_date between say.ay_calendar_start and say.ay_calendar_end) desc,
                m.mou_sign_date desc,
                m.mou_id desc
        ) as rn
    from {{ ref('int_crm__mous') }} m
    join school_academic_years say
        on m.partner_id = say.school_id
    where m.mou_sign_date is not null
        and m.mou_sign_date <= say.ay_calendar_end
        and (m.mou_end_date is null or m.mou_end_date >= say.ay_calendar_start)
),

mou_matched as (
    select school_academic_year_id, mou_id, mou_sign_date, originates_this_ay
    from mou_overlap
    where rn = 1
),

joined as (
    select
        say.school_academic_year_id,
        say.school_id,
        say.academic_year_id,
        say.academic_year,
        say.ay_end_year,
        say.ay_calendar_start,
        sdw.session_detail_start_date,
        sdw.session_detail_end_date,
        mm.mou_id,
        mm.mou_sign_date,
        mm.originates_this_ay
    from school_academic_years say
    left join session_detail_window sdw
        on say.school_academic_year_id = sdw.school_academic_year_id
    left join mou_matched mm
        on say.school_academic_year_id = mm.school_academic_year_id
),

resolved as (
    select
        school_academic_year_id,
        school_id,
        academic_year_id,
        academic_year,
        mou_id,
        mou_sign_date,
        case
            when session_detail_start_date is not null then session_detail_start_date
            when mou_sign_date is not null and originates_this_ay then (mou_sign_date + interval '60 days')::date
            when mou_sign_date is not null then ay_calendar_start
            else null
        end as window_start_date,
        case
            when session_detail_start_date is not null then session_detail_end_date
            when mou_sign_date is not null then make_date(ay_end_year, 3, 30)
            else null
        end as window_end_date,
        case
            when session_detail_start_date is not null then 'session_detail'
            when mou_sign_date is not null then 'mou_default'
            else 'no_data'
        end as window_source
    from joined
)

select
    school_academic_year_id,
    school_id,
    academic_year_id,
    academic_year,
    mou_id,
    mou_sign_date,
    case when window_start_date <= window_end_date then window_start_date end as window_start_date,
    case when window_start_date <= window_end_date then window_end_date end as window_end_date,
    case when window_start_date <= window_end_date then window_source else 'no_data' end as window_source
from resolved
