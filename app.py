import os
from datetime import timedelta
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")

@st.cache_data(ttl=600)
def q(sql):
    with engine.connect() as c:
        return pd.read_sql(sql, c)

st.title("NYC MTA Dashboard")

# ---------- Diagnostics / sanity checks ----------
with st.expander("🔍 Diagnostics (row counts & sample rows)"):
    counts = q("""
        with d as (select 'fact_ridership_daily'::text as t, count(*)::bigint as n from fact_ridership_daily)
        union all select 'dim_weather_daily', count(*) from dim_weather_daily
        union all select 'fact_subway_hourly', count(*) from fact_subway_hourly
        union all select 'dim_events_daily',  count(*) from dim_events_daily
    """)
    c1, c2, c3, c4 = st.columns(4)
    for col, row in zip([c1,c2,c3,c4], counts.itertuples(index=False)):
        with col:
            st.metric(row.t, int(row.n))

    st.caption("First few rows from each table (if present):")
    for name, sql in [
        ("fact_ridership_daily", "select * from fact_ridership_daily order by date asc, mode asc limit 5"),
        ("dim_weather_daily",    "select * from dim_weather_daily order by date asc limit 5"),
        ("fact_subway_hourly",   "select * from fact_subway_hourly order by date asc, hour asc, borough asc limit 5"),
        ("dim_events_daily",     "select * from dim_events_daily order by date asc, borough asc limit 5"),
    ]:
        df = q(sql)
        st.write(f"**{name}**")
        st.dataframe(df, use_container_width=True)

# ---------- Find available ranges (so charts don’t look at empty windows) ----------
ranges = q("""
    with
      d as (select min(date) as min_d, max(date) as max_d from fact_ridership_daily),
      h as (select min(date) as min_h, max(date) as max_h from fact_subway_hourly),
      e as (select min(date) as min_e, max(date) as max_e from dim_events_daily)
    select * from d, h, e
""")
(min_d, max_d, min_h, max_h, min_e, max_e) = ranges.iloc[0].tolist()

tab1, tab2, tab3 = st.tabs(["Daily", "Hourly", "Events"])

# ---------- DAILY ----------
with tab1:
    if pd.isna(max_d):
        st.info("No daily ridership loaded yet.")
    else:
        # Show the *entire* available daily window (or last 730 days if very long)
        start = max(min_d, max_d - timedelta(days=730)) if not pd.isna(min_d) else max_d - timedelta(days=730)
        df = q(f"""
            select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
            from vw_ridership_daily_joined
            where date between '{start}' and '{max_d}'
            order by date, mode
        """)
        if df.empty:
            st.warning("No daily rows in the selected window.")
        else:
            st.subheader("Systemwide Daily Ridership")
            st.line_chart(df.pivot(index="date", columns="mode", values="riders"))
            with st.expander("Show data"):
                st.dataframe(df, use_container_width=True)

# ---------- HOURLY ----------
with tab2:
    if pd.isna(max_h):
        st.info("No hourly subway data loaded yet.")
    else:
        # Center on the last available hourly date and show ±2 days
        start = max_h - timedelta(days=2)
        end   = max_h
        hh = q(f"""
            select date, hour, borough, riders
            from fact_subway_hourly
            where date between '{start}' and '{end}'
            order by date, hour, borough
        """)
        if hh.empty:
            st.warning("No hourly rows near the last available date.")
        else:
            st.subheader(f"Hourly Subway Ridership (ending {end})")
            st.dataframe(hh.head(500), use_container_width=True)

# ---------- EVENTS ----------
with tab3:
    if pd.isna(max_e):
        st.info("No events loaded yet.")
    else:
        # Show the full events window (or last 365 if long)
        start = max(min_e, max_e - timedelta(days=365)) if not pd.isna(min_e) else max_e - timedelta(days=365)
        ev = q(f"""
            select date, sum(event_count)::int as events
            from dim_events_daily
            where date between '{start}' and '{max_e}'
            group by date
            order by date
        """)
        if ev.empty:
            st.warning("No events in the selected window.")
        else:
            st.subheader("Permitted Events per Day")
            st.bar_chart(ev.set_index("date"))
            with st.expander("Show data"):
                st.dataframe(ev, use_container_width=True)
