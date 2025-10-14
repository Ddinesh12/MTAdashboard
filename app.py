# app.py (replace file)

import os
from datetime import date, timedelta

import altair as alt
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")
alt.data_transformers.disable_max_rows()

@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    with engine.connect() as c:
        return pd.read_sql(sql, c)

# ---------- helpers ----------
def last(n: int) -> date:
    return (date.today() - timedelta(days=n))

def to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date

# ---------- tabs ----------
tab_overview, tab_hourly, tab_diags = st.tabs(["Overview", "Hourly patterns", "Diagnostics"])

# ======================================================================
# 1) OVERVIEW
# ======================================================================
with tab_overview:
    st.header("Daily ridership (systemwide)")

    # pull last-12 months w/ weather + events + rolling fields
    daily = q("""
        select *
        from vw_ridership_daily_rolling
        where date >= current_date - interval '400 days'
        order by date, mode
    """)
    if daily.empty:
        st.info("No daily rows found. Did you run the backfill?")
        st.stop()

    daily["date"] = to_date(daily["date"])

    # --- layout: main area (chart) + right filter rail  ----------------
    col_main, col_filter = st.columns([4, 1])

    with col_filter:
        st.subheader("Filters")
        mode = st.radio("Mode", ["subway", "bus"], index=0, horizontal=True)
        weather_on = st.toggle("Show weather markers (wet/hot/cold)", value=True)
        events_on = st.toggle("Show events markers", value=True)

        # slider bounds from data; default to last 365d
        dmin, dmax = daily["date"].min(), daily["date"].max()
        default_start = max(dmin, last(365))
        drange = st.slider(
            "Date range", min_value=dmin, max_value=dmax,
            value=(default_start, dmax),
            format="MMM d, YYYY"
        )

    # filter by mode + range
    df = daily[(daily["mode"] == mode) &
               (daily["date"] >= drange[0]) &
               (daily["date"] <= drange[1])].copy()

    # KPI cards (last business-ish day vs 28d baseline)
    latest = df.sort_values("date").tail(1)
    if not latest.empty:
        c1, c2, c3 = st.columns(3)
        last_riders = int(latest["riders"].iloc[0])
        ma28 = latest["riders_ma28"].iloc[0] or None
        base180 = latest["riders_baseline_180"].iloc[0] or None
        pct_vs_180 = None if not base180 else (last_riders - base180)/base180

        c1.metric(f"{mode.title()} riders (latest day)", f"{last_riders:,}")
        c2.metric("28-day moving avg", f"{int(ma28):,}" if ma28 else "—")
        c3.metric("vs 180-day baseline", f"{pct_vs_180:+.1%}" if pct_vs_180 is not None else "—")

    # Altair line w/ optional markers
    base = alt.Chart(df).encode(x="date:T")
    line = base.mark_line().encode(y=alt.Y("riders:Q", title="Riders"))

    ma7 = base.mark_line(strokeDash=[4,3], opacity=0.8).encode(
        y=alt.Y("riders_ma7:Q", title="7d MA")
    )

    layers = [line, ma7]

    if weather_on:
        wet = base.transform_filter(alt.datum.wet_day == True).mark_point(size=30, shape="triangle-up").encode(
            color=alt.value("#4e79a7"), tooltip=["date:T","tmax_f","prcp_in"]
        )
        hot = base.transform_filter(alt.datum.hot_day == True).mark_point(size=30, shape="circle").encode(
            color=alt.value("#e15759"), tooltip=["date:T","tmax_f","prcp_in"]
        )
        cold = base.transform_filter(alt.datum.cold_day == True).mark_point(size=30, shape="diamond").encode(
            color=alt.value("#76b7b2"), tooltip=["date:T","tmax_f","prcp_in"]
        )
        layers += [wet, hot, cold]

    if events_on:
        ev = base.transform_calculate(ev="toNumber(datum.event_count)").transform_filter(
            alt.datum.ev > 0
        ).mark_tick(size=30, thickness=2).encode(
            y="riders:Q",
            color=alt.value("#f28e2b"),
            tooltip=["date:T", alt.Tooltip("event_count:Q", title="Events")]
        )
        layers.append(ev)

    chart = alt.layer(*layers).properties(height=380)
    col_main.altair_chart(chart, use_container_width=True)

# ======================================================================
# 2) HOURLY PATTERNS (Subway 2025+)
# ======================================================================
with tab_hourly:
    st.header("Subway hourly patterns (2025+)")

    # tiny “left nav” inside the tab (two columns)
    nav, area = st.columns([1, 4])

    with nav:
        view = st.radio("Sections", ["Typical day profile", "Rush-hour multiplier"], index=0)

    # preload bases
    hourly60 = q("""
        select * from vw_hourly_last60
        order by date, hour, borough
    """)
    hourly60["date"] = to_date(hourly60["date"])

    if view == "Typical day profile":
        with area:
            rcol, fcol = st.columns([4, 1])
            with fcol:
                borough = st.selectbox("Borough", ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"], index=2)
                kind = st.radio("Day type", ["Weekday", "Weekend"], index=0, horizontal=True)

            # compute mean by hour for selected borough + day type
            is_wknd = (kind == "Weekend")
            base = hourly60[(hourly60["borough"] == borough)]
            base["is_weekend"] = pd.to_datetime(base["date"]).weekday.isin([5,6])
            curve = (base[base["is_weekend"] == is_wknd]
                     .groupby("hour", as_index=False)["riders"].mean())

            if curve.empty:
                st.warning("No rows for that selection in the last 60 days.")
            else:
                ch = alt.Chart(curve).mark_line(point=True).encode(
                    x=alt.X("hour:O", title="Hour of day"),
                    y=alt.Y("riders:Q", title="Avg riders"),
                    tooltip=["hour","riders"]
                ).properties(height=380)
                rcol.altair_chart(ch, use_container_width=True)

    else:  # Rush-hour multiplier
        with area:
            rush = q("""
                select * from vw_rush_hour_multiplier
                where date >= current_date - interval '60 days'
                order by date, borough
            """)
            rush["date"] = to_date(rush["date"])
            boro_sel = st.multiselect(
                "Boroughs",
                ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"],
                default=["Manhattan","Brooklyn","Queens"]
            )
            rr = rush[rush["borough"].isin(boro_sel)].copy()
            if rr.empty:
                st.warning("No rows for selected boroughs.")
            else:
                ch = alt.Chart(rr).mark_line().encode(
                    x="date:T",
                    y=alt.Y("rush_hour_multiplier:Q", title="Peak / Avg"),
                    color="borough:N",
                    tooltip=["date:T","borough","peak_hourly","avg_hourly","rush_hour_multiplier"]
                ).properties(height=380)
                st.altair_chart(ch, use_container_width=True)

# ======================================================================
# 3) DIAGNOSTICS
# ======================================================================
with tab_diags:
    st.subheader("Diagnostics")
    # show any SQL error nicely
    try:
        counts = q("""
            with d as (select 'fact_ridership_daily'::text as t, count(*)::bigint as n from fact_ridership_daily)
            union all select 'dim_weather_daily',count(*) from dim_weather_daily
            union all select 'fact_subway_hourly',count(*) from fact_subway_hourly
            union all select 'dim_events_daily', count(*) from dim_events_daily
        """)
        st.dataframe(counts, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Query failed: {getattr(e, 'orig', e)}")

    c1, c2, c3, c4 = st.columns(4)
    c1.caption("ridership_daily (latest 5)")
    c1.dataframe(q("select * from fact_ridership_daily order by date desc, mode limit 5"),
                 use_container_width=True, hide_index=True)
    c2.caption("weather_daily (latest 5)")
    c2.dataframe(q("select * from dim_weather_daily order by date desc limit 5"),
                 use_container_width=True, hide_index=True)
    c3.caption("subway_hourly (latest 5)")
    c3.dataframe(q("select * from fact_subway_hourly order by date desc, hour desc, borough limit 5"),
                 use_container_width=True, hide_index=True)
    c4.caption("events_daily (latest 5)")
    c4.dataframe(q("select * from dim_events_daily order by date desc, borough limit 5"),
                 use_container_width=True, hide_index=True)
