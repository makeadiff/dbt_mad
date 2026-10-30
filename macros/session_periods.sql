{# Periods for the session deep dive tables (spec DO §3.7 R4, as Metric 3): "Since 1 October", "Last 28 days",
   and each calendar month from October 2026. Today is the IST date. Dalgo can't compute periods, so the
   tables are precomputed per period. #}
{% macro session_periods() %}
    with params as (
        select
            '2026-10-01'::date as tracking_start,
            (now() at time zone 'Asia/Kolkata')::date as today
    )
    select 'since_1_oct' as period_type, 'Since 1 October' as period_label, 1 as period_sort,
        p.tracking_start as period_start, p.today as period_end
    from params p
    union all
    select 'last_28_days', 'Last 28 days', 2, greatest(p.today - 27, p.tracking_start), p.today
    from params p
    union all
    select 'month', to_char(m, 'FMMonth YYYY'), 10 + row_number() over (order by m)::int,
        m::date, least((m + interval '1 month - 1 day')::date, p.today)
    from params p
    cross join generate_series(p.tracking_start, date_trunc('month', p.today), interval '1 month') as m
{% endmacro %}
