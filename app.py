# app.py
import os
from datetime import date, datetime, timedelta

import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")

@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    with engine.connect() as c:
        return pd.read_sql(sql, c)

def pct(a, b):
    if b in (0, None) or pd.isna(b):
        return None
    return (a - b) / b

# -----------------------------
# Sidebar (left nav)
# -----------------------------
st.sidebar.header("Pages")
page = st.sidebar.radio(
    "Search for pages on the website",
    ["Overview", "Hourly Patterns", "Diagnostics"],
    label_visibility="collapsed",
    key="nav_page",
)

# Small style tweaks
st.markdown(
    """
    <style>
    section[data-testid="stSidebar"] { width: 320px !important; }
    div[data-testid="stMetricValue"] { font-size: 30px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================================================
# 1) OVERVIEW
# =========================================================
if page == "Overview":
    st.title("Daily Ridership and Traffic")

    # --- pull daily rolling + joined once (we’ll filter locally)
    daily = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day,
               riders_ma7, riders_ma28, riders_baseline_180
        from vw_ridership_daily_rolling
        order by date, mode
    """)

    if daily.empty:
        st.warning("No daily data available yet.")
        st.stop()

    # --- date slider (pure datetime to avoid type-mix issues)
    dmin = daily["date"].min()
    dmax = daily["date"].max()

    min_dt = pd.to_datetime(dmin).to_pydatetime()
    max_dt = pd.to_datetime(dmax).to_pydatetime()
    default_start = max_dt - timedelta(days=365)
    val0 = max(min_dt, default_start)
    val1 = max_dt

    st.caption("Select a start and end date")
    drange_dt = st.slider(
        "Date range",
        min_value=min_dt,
        max_value=max_dt,
        value=(val0, val1),
        step=timedelta(days=1),
        format="MMM DD, YYYY",
        key="ov_range",
        label_visibility="collapsed",
    )
    drange = (drange_dt[0].date(), drange_dt[1].date())

    # --- mode switch (single series to keep the chart clean)
    mode = st.radio("Mode", ["subway", "bus"], horizontal=True, key="ov_mode")

    df = daily[(daily["date"] >= drange[0]) & (daily["date"] <= drange[1]) & (daily["mode"] == mode)]

    # --- KPI row (last available day in window)
    last_day = df["date"].max()
    last_row = df.loc[df["date"] == last_day].head(1)

    colA, colB, colC = st.columns(3)
    if not last_row.empty:
        lr = last_row.iloc[0]
        colA.metric(
            f"{mode.title()} riders (last day)",
            f"{int(lr['riders']):,}",
            None,
        )
        if not pd.isna(lr["riders_ma28"]):
            delta_vs_ma28 = pct(lr["riders"], lr["riders_ma28"])
            colB.metric(
                "vs. 28-day MA",
                f"{lr['riders_ma28']:.0f}",
                f"{delta_vs_ma28*100:+.1f}%",
            )
        else:
            colB.metric("vs. 28-day MA", "—", None)

        if not pd.isna(lr["riders_baseline_180"]):
            delta_vs_180 = pct(lr["riders"], lr["riders_baseline_180"])
            colC.metric(
                "vs. 180-day baseline",
                f"{lr['riders_baseline_180']:.0f}",
                f"{delta_vs_180*100:+.1f}%",
            )
        else:
            colC.metric("vs. 180-day baseline", "—", None)
    else:
        colA.metric(f"{mode.title()} riders (last day)", "—")
        colB.metric("vs. 28-day MA", "—")
        colC.metric("vs. 180-day baseline", "—")

    # --- main trend chart
    st.subheader("Daily trend")
    if df.empty:
        st.info("No rows in the selected window. Try widening the date range.")
    else:
        chart_df = df[["date", "riders"]].set_index("date")
        st.line_chart(chart_df)

    # --- data table (optional)
    with st.expander("Show data"):
        st.dataframe(
            df[["date", "mode", "riders", "wet_day", "hot_day", "cold_day", "riders_ma7", "riders_ma28", "riders_baseline_180"]]
            .sort_values("date"),
            use_container_width=True,
        )

# =========================================================
# 2) HOURLY PATTERNS (2025+)
# =========================================================
elif page == "Hourly Patterns":
    st.title("Hourly Patterns (Subway, 2025+)")

    # Base hourly window
    base = q("""
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
    """)
    if base.empty:
        st.warning("No hourly rows in the last 60 days (remember: we’ve only loaded 2025+).")
        st.stop()

    # flags for weekday/weekend
    base["date"] = pd.to_datetime(base["date"])
    base["is_weekend"] = base["date"].dt.dayofweek.isin([5, 6])

    all_boros = ["All boroughs", "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]

    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        part = st.radio("Day type", ["Weekday", "Weekend"], horizontal=True, key="hp_daytype")
    with c2:
        sel_boro = st.selectbox("Borough", all_boros, key="hp_boro")
    with c3:
        pass

    # Filtered subset for typical profile
    if part == "Weekend":
        sub = base[base["is_weekend"]]
    else:
        sub = base[~base["is_weekend"]]

    if sel_boro != "All boroughs":
        sub = sub[sub["borough"] == sel_boro]

    st.subheader("Typical day profile")
    if sub.empty:
        st.info("No rows for that selection in the last 60 days.")
    else:
        prof = (
            sub.groupby("hour", as_index=False)["riders"]
               .mean()
               .sort_values("hour")
        )
        st.line_chart(prof.set_index("hour"))

    # Rush-hour multiplier trend (by borough; can show all)
    rh = q("""
        select date, borough, rush_hour_multiplier, peak_hourly, avg_hourly
        from vw_rush_hour_multiplier
        where date >= (current_date - interval '60 days')
        order by date, borough
    """)
    st.subheader("Rush-hour multiplier trend (peak / average)")
    if rh.empty:
        st.info("No rush-hour data for the last 60 days.")
    else:
        if sel_boro == "All boroughs":
            # multi-line: each borough
            piv = rh.pivot_table(index="date", columns="borough", values="rush_hour_multiplier")
            st.line_chart(piv)
        else:
            one = rh[rh["borough"] == sel_boro][["date", "rush_hour_multiplier"]].set_index("date")
            if one.empty:
                st.info("No rows for that borough in the last 60 days.")
            else:
                st.line_chart(one)

    with st.expander("Show hourly sample"):
        st.dataframe(base.tail(200), use_container_width=True)

# =========================================================
# 3) DIAGNOSTICS
# =========================================================
else:
    st.title("Diagnostics")

    # quick counts
    cnt = q("""
        with d as (select 'fact_ridership_daily'::text as t, count(*)::bigint as n from fact_ridership_daily),
             w as (select 'dim_weather_daily', count(*) from dim_weather_daily),
             h as (select 'fact_subway_hourly', count(*) from fact_subway_hourly),
             e as (select 'dim_events_daily',  count(*) from dim_events_daily)
        select * from d
        union all select * from w
        union all select * from h
        union all select * from e
        order by t
    """)
    st.write(cnt)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        r = q("""select * from fact_ridership_daily order by date desc, mode limit 5""")
        st.caption("ridership_daily (latest 5)")
        st.dataframe(r, use_container_width=True)
    with col2:
        w = q("""select * from dim_weather_daily order by date desc limit 5""")
        st.caption("weather_daily (latest 5)")
        st.dataframe(w, use_container_width=True)
    with col3:
        h = q("""select * from fact_subway_hourly order by date desc, hour desc, borough limit 5""")
        st.caption("subway_hourly (latest 5)")
        st.dataframe(h, use_container_width=True)
    with col4:
        e = q("""select * from dim_events_daily order by date desc, borough limit 5""")
        st.caption("events_daily (latest 5)")
        st.dataframe(e, use_container_width=True)
