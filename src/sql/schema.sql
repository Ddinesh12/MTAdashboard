-- =========================================================
-- NYC MTA Dashboard — Database Schema
-- Tables: daily ridership, hourly subway, weather, (optional) events
-- Target: Postgres/Neon
-- =========================================================

set search_path to public;

-- ---------------------------
-- WEATHER (daily, Central Park by default)
-- ---------------------------
create table if not exists dim_weather_daily (
  date        date primary key,
  station_id  text,
  tmax_f      numeric,
  tmin_f      numeric,
  prcp_in     numeric,
  snow_in     numeric
);

create index if not exists idx_weather_date on dim_weather_daily(date);

comment on table dim_weather_daily is 'NOAA/NCEI daily summaries (e.g., Central Park USW00094728). Units in Fahrenheit/inches.';


-- ---------------------------
-- RIDERSHIP (daily, systemwide by mode)
-- Two rows per date: subway + bus
-- ---------------------------
create table if not exists fact_ridership_daily (
  date   date not null,
  mode   text not null check (mode in ('subway','bus')),
  riders bigint not null,
  source text,
  primary key (date, mode)
);

create index if not exists idx_ridership_daily_date on fact_ridership_daily(date);
create index if not exists idx_ridership_daily_mode on fact_ridership_daily(mode);

comment on table fact_ridership_daily is 'Daily systemwide ridership by mode (subway/bus) from MTA Open NY.';


-- ---------------------------
-- SUBWAY HOURLY (aggregated to date × hour × borough)
-- ---------------------------
create table if not exists fact_subway_hourly (
  date     date not null,
  hour     int  not null check (hour between 0 and 23),
  borough  text not null check (borough in ('Bronx','Brooklyn','Manhattan','Queens','Staten Island')),
  riders   bigint not null,
  source   text,
  primary key (date, hour, borough)
);

create index if not exists idx_subway_hourly_date on fact_subway_hourly(date);
create index if not exists idx_subway_hourly_borough on fact_subway_hourly(borough);
create index if not exists idx_subway_hourly_date_borough on fact_subway_hourly(date, borough);

comment on table fact_subway_hourly is 'Hourly subway ridership aggregated to (date, hour, borough) using MTA hourly datasets (2020–2024 + 2025+).';


-- ---------------------------
-- NYC EVENTS (optional; daily counts per borough)
-- Keep the table even if you don’t load it yet, so views can LEFT JOIN it later.
-- ---------------------------
create table if not exists dim_events_daily (
  date        date not null,
  borough     text not null check (borough in ('Bronx','Brooklyn','Manhattan','Queens','Staten Island')),
  event_count int  not null default 0,
  primary key (date, borough)
);

create index if not exists idx_events_date on dim_events_daily(date);
create index if not exists idx_events_borough on dim_events_daily(borough);

comment on table dim_events_daily is 'Daily permitted event counts per borough (NYC Open Data).';
