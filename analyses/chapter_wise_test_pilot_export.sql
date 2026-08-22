-- chapter_wise_test_pilot_export
-- One-off data export (plain SQL, no dbt ref/compile needed) for the WhatsApp-bot (DOTS) pilot
-- case-study review. One row per chapter that actually has DOTS bot data in the analysis window.
--
-- HOW TO RUN
--   Paste directly into your warehouse SQL client (psql, DBeaver, etc.) against mad_data_warehouse
--   and export the result set as CSV.
--
-- SCHEMA
--   int_dots__child_attendance / int_dots__volunteer_attendance live in "prod_silver_dots".
--   dim_chapter_mapping / dim_chapter_current_status / fct_e2_school_coverage live in "prod_gold_marts".
--   If validating against dev instead, replace "prod_silver_dots" -> "dev_silver_dots" and
--   "prod_gold_marts" -> "dev_gold_marts" throughout.
--
-- BUILT FROM THE SILVER LAYER DIRECTLY (per request) -- not the pre-aggregated marts -- so the
-- day-of-week snap is fully visible here rather than hidden inside fct_e2_*_by_slot_date.
-- WHY THE SNAP: DOTS submissions are logged via entry_last_updated_date, which is typically a few
-- days AFTER the actual session (volunteers/children submit late). day_of_week on the row tells you
-- which weekday the session actually falls on, so we walk entry_last_updated_date backward to the
-- nearest previous occurrence of that weekday -- that's the real session_date.
--
-- "Test chapters" are auto-discovered: any chapter with at least one DOTS submission (child or
-- volunteer) whose snapped session_date falls inside the analysis period below. This replaces
-- manually pasting a chapter list -- if a chapter has bot data in this window, it shows up here.
-- To restrict to a specific sheet-approved list instead, add `where chapter_id in (...)` at the
-- very bottom.

with analysis_weeks as (
    -- >>> EDIT ME: Week 1-4 ranges from the shared sheet (columns O-T), 2026 <<<
    select * from (values
        (1, '2026-02-20'::date, '2026-02-28'::date),   -- Actuals Week 1 (20/02-28/02)
        (2, '2026-02-28'::date, '2026-03-06'::date),   -- Actuals Week 2 (28/02-06/03)
        (3, '2026-03-07'::date, '2026-03-13'::date),   -- Actuals Week 3 (07/03-13/03)
        (4, '2026-03-14'::date, '2026-03-20'::date)    -- Actuals Week 4 (14/03-20/03)
    ) as w(week_number, week_start, week_end)
),

period_bounds as (
    select
        min(week_start) as period_start,
        max(week_end) as period_end,
        count(*) as total_weeks_expected,
        to_char(min(week_start), 'DD/MM/YYYY') || ' - ' || to_char(max(week_end), 'DD/MM/YYYY') as analysis_period
    from analysis_weeks
),

dow_number as (
    select * from (values
        ('Sunday', 0), ('Monday', 1), ('Tuesday', 2), ('Wednesday', 3),
        ('Thursday', 4), ('Friday', 5), ('Saturday', 6)
    ) as d(day_name, day_number)
),

-- Snap every DOTS child-attendance submission back to its actual session date
child_snapped as (
    select
        ca.record_id,
        ca.partner_id::text as chapter_id,
        ca.child_id,
        ca.slot_class_section_id,
        upper(trim(ca.attendance)) = 'TRUE' as attended,
        ca.entry_last_updated_date::date
            - ((extract(dow from ca.entry_last_updated_date::date)::int - dn.day_number + 7) % 7) as session_date
    from prod_silver_dots.int_dots__child_attendance ca
    left join dow_number dn
        on trim(ca.day_of_week) = dn.day_name
    where ca.entry_last_updated_date is not null
        and ca.partner_id is not null
),

-- Snap every DOTS volunteer-attendance submission back to its actual session date
volunteer_snapped as (
    select
        va.record_id,
        va.partner_id::text as chapter_id,
        va.volunteer_id,
        va.slot_class_section_id,
        upper(trim(va.attendance)) = 'TRUE' as attended,
        va.entry_last_updated_date::date
            - ((extract(dow from va.entry_last_updated_date::date)::int - dn.day_number + 7) % 7) as session_date
    from prod_silver_dots.int_dots__volunteer_attendance va
    left join dow_number dn
        on trim(va.day_of_week) = dn.day_name
    where va.entry_last_updated_date is not null
        and va.partner_id is not null
),

-- Bucket each snapped session_date into Week 1-4 using the literal ranges from analysis_weeks
child_in_period as (
    select cs.*, aw.week_number
    from child_snapped cs
    join analysis_weeks aw
        on cs.session_date between aw.week_start and aw.week_end
),

volunteer_in_period as (
    select vs.*, aw.week_number
    from volunteer_snapped vs
    join analysis_weeks aw
        on vs.session_date between aw.week_start and aw.week_end
),

test_chapters as (
    select chapter_id from child_in_period
    union
    select chapter_id from volunteer_in_period
),

chapter_info as (
    select
        cm.chapter_id,
        cm.chapter_name,
        cm.city_name,
        cm.engine,
        ccs.latest_academic_year
    from prod_gold_marts.dim_chapter_mapping cm
    left join prod_gold_marts.dim_chapter_current_status ccs
        on cm.chapter_id = ccs.school_id::text
),

total_children as (
    select chapter_id, academic_year, total_children_in_system
    from prod_gold_marts.fct_e2_school_coverage
),

-- All-time month footprint of bot data per chapter, used only to check coverage (1 vs 3 months etc.)
bot_months as (
    select chapter_id, date_trunc('month', session_date)::date as activity_month from child_snapped
    union
    select chapter_id, date_trunc('month', session_date)::date from volunteer_snapped
),

bot_coverage as (
    -- Lookback window anchors on period_end's month (not period_start's) -- the analysis window can
    -- span two calendar months (e.g. 20/02-20/03), and anchoring on period_start would silently drop
    -- the later month's data from the coverage check.
    select
        tc.chapter_id,
        count(distinct bm.activity_month) filter (
            where bm.activity_month between date_trunc('month', pb.period_end) - interval '2 months'
                                         and date_trunc('month', pb.period_end)
        ) as months_with_bot_data_last3,
        string_agg(
            distinct to_char(bm.activity_month, 'YYYY-MM'), ', ' order by to_char(bm.activity_month, 'YYYY-MM')
        ) filter (
            where bm.activity_month between date_trunc('month', pb.period_end) - interval '2 months'
                                         and date_trunc('month', pb.period_end)
        ) as bot_data_months_list
    from test_chapters tc
    cross join period_bounds pb
    left join bot_months bm on bm.chapter_id = tc.chapter_id
    group by tc.chapter_id
),

children_with_bot as (
    select chapter_id, count(distinct child_id) as children_with_bot_entries
    from child_in_period
    group by chapter_id
),

sessions_conducted as (
    select chapter_id, count(distinct slot_class_section_id::text || '_' || session_date::text) as sessions_conducted
    from (
        select chapter_id, slot_class_section_id, session_date from child_in_period
        union
        select chapter_id, slot_class_section_id, session_date from volunteer_in_period
    ) s
    group by chapter_id
),

volunteer_consistency as (
    select
        chapter_id,
        count(distinct volunteer_id) as volunteers_with_bot_entries,
        round(avg(weeks_attended::numeric), 2) as avg_volunteer_weeks_attended
    from (
        select chapter_id, volunteer_id, count(distinct week_number) as weeks_attended
        from volunteer_in_period
        where attended
        group by chapter_id, volunteer_id
    ) vi
    group by chapter_id
),

child_consistency as (
    select
        chapter_id,
        count(distinct child_id) as children_attended_at_least_once,
        round(avg(weeks_attended::numeric), 2) as avg_child_weeks_attended
    from (
        select chapter_id, child_id, count(distinct week_number) as weeks_attended
        from child_in_period
        where attended
        group by chapter_id, child_id
    ) ci
    group by chapter_id
),

-- Raw DOTS submission count (child + volunteer, any attendance value) per chapter per week -- matches
-- the sheet's own "Actuals Week N ... BOT Entries" columns literally. Note this counts entries logged,
-- not unique sessions (sessions_conducted above dedupes to one per slot_class_section+date instead).
weekly_bot_entries as (
    select chapter_id, week_number, count(*) as bot_entries
    from (
        select chapter_id, record_id, week_number from child_in_period
        union all
        select chapter_id, record_id, week_number from volunteer_in_period
    ) e
    group by chapter_id, week_number
),

weekly_bot_entries_pivoted as (
    -- >>> EDIT ME alongside analysis_weeks above: keep these week numbers/date labels in sync <<<
    select
        chapter_id,
        coalesce(max(bot_entries) filter (where week_number = 1), 0) as "Week 1 (20/02-28/02) BOT Entries",
        coalesce(max(bot_entries) filter (where week_number = 2), 0) as "Week 2 (28/02-06/03) BOT Entries",
        coalesce(max(bot_entries) filter (where week_number = 3), 0) as "Week 3 (07/03-13/03) BOT Entries",
        coalesce(max(bot_entries) filter (where week_number = 4), 0) as "Week 4 (14/03-20/03) BOT Entries"
    from weekly_bot_entries
    group by chapter_id
),

combined as (
    select
        tc.chapter_id,
        ci.chapter_name,
        ci.city_name,
        ci.engine,
        tch.total_children_in_system as total_children,
        cwb.children_with_bot_entries,
        pb.analysis_period,
        pb.total_weeks_expected,
        round(vc.avg_volunteer_weeks_attended / nullif(pb.total_weeks_expected, 0) * 100, 1)
            as volunteer_consistency_pct,
        round(cc.avg_child_weeks_attended / nullif(pb.total_weeks_expected, 0) * 100, 1)
            as child_consistency_pct,
        coalesce(sc.sessions_conducted, 0) as sessions_conducted,
        coalesce(wbe."Week 1 (20/02-28/02) BOT Entries", 0) as "Week 1 (20/02-28/02) BOT Entries",
        coalesce(wbe."Week 2 (28/02-06/03) BOT Entries", 0) as "Week 2 (28/02-06/03) BOT Entries",
        coalesce(wbe."Week 3 (07/03-13/03) BOT Entries", 0) as "Week 3 (07/03-13/03) BOT Entries",
        coalesce(wbe."Week 4 (14/03-20/03) BOT Entries", 0) as "Week 4 (14/03-20/03) BOT Entries",
        coalesce(bcov.months_with_bot_data_last3, 0) as bot_data_months_count,
        bcov.bot_data_months_list,
        case
            when coalesce(bcov.months_with_bot_data_last3, 0) >= 3 then '3-month coverage'
            when coalesce(bcov.months_with_bot_data_last3, 0) between 1 and 2
                then bcov.months_with_bot_data_last3 || '-month coverage'
            else 'No usable bot data'
        end as bot_data_coverage_status
    from test_chapters tc
    cross join period_bounds pb
    left join chapter_info ci on tc.chapter_id = ci.chapter_id
    left join total_children tch
        on tc.chapter_id = tch.chapter_id and ci.latest_academic_year = tch.academic_year
    left join children_with_bot cwb on tc.chapter_id = cwb.chapter_id
    left join sessions_conducted sc on tc.chapter_id = sc.chapter_id
    left join volunteer_consistency vc on tc.chapter_id = vc.chapter_id
    left join child_consistency cc on tc.chapter_id = cc.chapter_id
    left join bot_coverage bcov on tc.chapter_id = bcov.chapter_id
    left join weekly_bot_entries_pivoted wbe on tc.chapter_id = wbe.chapter_id
)

select
    *,
    -- top 3 chapters by combined volunteer/child consistency, session volume, and bot coverage --
    -- for the case-study shortlist
    case
        when dense_rank() over (
            order by
                (coalesce(volunteer_consistency_pct, 0) + coalesce(child_consistency_pct, 0)) desc,
                sessions_conducted desc,
                bot_data_months_count desc
        ) <= 3
        then 'Case Study Candidate'
        else ''
    end as case_study_flag
from combined
order by
    (coalesce(volunteer_consistency_pct, 0) + coalesce(child_consistency_pct, 0)) desc,
    sessions_conducted desc
