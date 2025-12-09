-- =========================================================
-- NYC MTA Dashboard — Logical Views
-- Purpose-built views for charts/KPIs (pre-joined and pre-aggregated data)
-- =========================================================

set search_path to public;

-- ---------------------------
-- Add basic weather flags per day
-- wet/hot/cold based on thresholds
-- ---------------------------
create or replace view vw_weather_flags as
select
  w.*,
  (prcp_in is not null and prcp_in > 0)                as wet_day,   -- any precipitation
  (tmax_f is not null and tmax_f >= 85)                as hot_day,   -- >= 85°F considered hot
  (tmax_f is not null and tmax_f <= 32)                as cold_day   -- <= 32°F considered cold
from dim_weather_daily w;


-- ---------------------------
-- Join daily ridership to weather
-- plus optional events (summed systemwide per day)
-- ---------------------------
create or replace view vw_ridership_daily_joined as
select
  r.date,
  r.mode,
  r.riders,
  w.tmax_f,
  w.prcp_in,
  w.wet_day,
  w.hot_day,
  w.cold_day,
  coalesce(e.event_count, 0) as event_count   -- default 0 if no events data
from fact_ridership_daily r
left join vw_weather_flags w  using (date)   -- join weather by date
left join (
  -- convert borough-level events into total events per day
  select date, sum(event_count)::int as event_count
  from dim_events_daily
  group by date
) e using (date);


-- ---------------------------
-- Rolling averages: 7-day, 28-day, 180-day baselines
-- per mode (subway vs bus)
-- ---------------------------
create or replace view vw_ridership_daily_rolling as
select
  d.*,
  -- 7-day moving average (include current day)
  avg(riders) over (
    partition by mode
    order by date
    rows between 6 preceding and current row
  )::numeric        as riders_ma7,

  -- 28-day moving average
  avg(riders) over (
    partition by mode
    order by date
    rows between 27 preceding and current row
  )::numeric        as riders_ma28,

  -- baseline: previous 180 days (exclude current day)
  avg(riders) over (
    partition by mode
    order by date
    rows between 180 preceding and 1 preceding
  )::numeric        as riders_baseline_180
from vw_ridership_daily_joined d;


-- ---------------------------
-- % delta vs baseline:
-- Compare today vs trailing 180-day baseline
-- ---------------------------
create or replace view vw_ridership_effects as
select
  date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count,
  case
    when riders_baseline_180 is null or riders_baseline_180 = 0 then null
    -- (riders - baseline) / baseline → % above/below normal
    else (riders - riders_baseline_180) / riders_baseline_180
  end as pct_delta_vs_180
from vw_ridership_daily_rolling;


-- ---------------------------
-- Break hourly subway into weekday/weekend flags
-- adds day-of-week (0-6) and weekend boolean
-- ---------------------------
create or replace view vw_subway_hourly_base as
select
  s.date,
  extract(dow from s.date)::int as dow,  -- PostgreSQL day-of-week (0=Sunday)
  case when extract(dow from s.date)::int in (0,6) then true else false end as is_weekend,
  s.hour,
  s.borough,
  s.riders
from fact_subway_hourly s;


-- ---------------------------
-- Rush Hour Multiplier:
-- peak hourly ridership divided by average hourly ridership
-- per day & borough
-- ---------------------------
create or replace view vw_rush_hour_multiplier as
with daily as (
  select
    date,
    borough,
    avg(riders)::numeric as avg_hourly,   -- mean of 24 hours
    max(riders)          as peak_hourly   -- busiest hour
  from vw_subway_hourly_base
  group by 1,2
)
select
  date,
  borough,
  case when avg_hourly = 0 then null else peak_hourly / avg_hourly end as rush_hour_multiplier,
  peak_hourly,
  avg_hourly
from daily;


-- ---------------------------
-- Weekend Factor:
-- By hour, compare average weekend ridership vs weekday ridership
-- (per borough)
-- ---------------------------
create or replace view vw_weekend_factor as
with split as (
  select
    borough,
    hour,
    avg(case when is_weekend then riders end)::numeric as wknd,  -- avg weekend hour
    avg(case when not is_weekend then riders end)::numeric as wkdy  -- avg weekday hour
  from vw_subway_hourly_base
  group by 1,2
)
select
  borough,
  hour,
  case when wkdy = 0 then null else wknd / wkdy end as weekend_factor,  -- ratio
  wknd as weekend_avg,
  wkdy as weekday_avg
from split;


-- ---------------------------
-- Hourly anomalies (z-score):
-- how unusual each hour is vs trailing window of same hour & borough
-- ---------------------------
create or replace view vw_hourly_anomalies as
select
  date,
  hour,
  borough,
  riders,
  (
    riders - avg(riders) over (
      partition by borough, hour
      order by date
      rows between 28 preceding and 1 preceding   -- exclude today
    )
  ) / nullif(
    stddev_pop(riders) over (
      partition by borough, hour
      order by date
      rows between 28 preceding and 1 preceding
    ), 0
  ) as zscore_28
from vw_subway_hourly_base;


-- ---------------------------
-- Daily last 12 months (useful filter window)
-- ---------------------------
create or replace view vw_daily_last12 as
select *
from vw_ridership_daily_rolling
where date >= (current_date - interval '365 days')
order by date;


-- ---------------------------
-- Hourly last 60 days (useful filter window)
-- ---------------------------
create or replace view vw_hourly_last60 as
select *
from vw_subway_hourly_base
where date >= (current_date - interval '60 days')
order by date, hour, borough;
