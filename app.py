# app.py
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

def to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date

def last(n: int) -> date:
    return date.today() - timedelta(days=n)

# ---------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------
tab_overview, tab_hourly, tab_weather, tab_diags = st.tabs(
    ["Overview", "Hourly patterns", "Weather & Events", "Diagnostics"]
)

# =====================================================================
# OVERVIEW
# =====================================================================
with tab_overview:
    st.header("Daily Ridership and Traffic")

    # Pull a wide window so slider feels responsive
    daily = q("""
        select *
        from vw_ridership_daily_rolling
        where date >= current_date - interval '400 days'
        order by date, mode
    """)
    if daily.empty:
        st.info("No daily rows found. Run backfill first.")
        st.stop()

    daily["date"] = to_date(daily["date"])

    # Right-rail filters
    main, rail = st.columns([4, 1])
    with rail:
        st.subheader("Filters")
        mode = st.radio("Mode", ["subway", "bus"], horizontal=True, index=0)
        dmin, dmax = daily["date"].min(), daily["date"].max()
        drange = st.slider(
            "Date range",
            min_value=dmin,
            max_value=dmax,
            value=(max(dmin, last(365)), dmax),
            format="MMM d, YYYY",
        )

    df = daily[
        (daily["mode"] == mode)
        & (daily["date"] >= drange[0])
        & (daily["date"] <= drange[1])
    ].copy()

    # KPI cards (latest vs 28d MA / 180d baseline)
    latest = df.sort_values("date").tail(1)
    if not latest.empty:
        c1, c2, c3 = st.columns(3)
        last_riders = int(latest["riders"].iloc[0])
        ma28 = latest["riders_ma28"].iloc[0]
        base180 = latest["riders_baseline_180"].iloc[0]
        pct_vs_180 = None if not base180 else (last_riders - base180) / base180
        c1.metric(f"{mode.title()} riders (latest day)", f"{last_riders:,}")
        c2.metric("28-day moving avg", f"{int(ma28):,}" if pd.notna(ma28) else "—")
        c3.metric("vs 180-day baseline", f"{pct_vs_180:+.1%}" if pct_vs_180 is not None else "—")

    # Clean line with optional 7d MA
    base = alt.Chart(df).encode(x="date:T")
    line = base.mark_line().encode(y=alt.Y("riders:Q", title="Riders"))
    ma7 = base.mark_line(strokeDash=[4,3], opacity=0.8).encode(y=alt.Y("riders_ma7:Q", title="7-day MA"))
    chart = alt.layer(line, ma7).properties(height=380)

    main.altair_chart(chart, use_container_width=True)

# =====================================================================
# HOURLY PATTERNS
# =====================================================================
with tab_hourly:
    st.header("Subway hourly patterns (2025+)")

    # mini left nav inside tab
    nav, area = st.columns([1, 4])
    with nav:
        subview = st.radio("Sections", ["Typical day profile", "Rush-hour multiplier"])

    hourly60 = q("""
        select * from vw_hourly_last60
        order by date, hour, borough
    """)
    hourly60["date"] = to_date(hourly60["date"])

    if subview == "Typical day profile":
        with area:
            plot, filt = st.columns([4, 1])
            with filt:
                borough = st.selectbox(
                    "Borough",
                    ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"],
                    index=2
                )
                daytype = st.radio("Day type", ["Weekday", "Weekend"], horizontal=True)

            base = hourly60[hourly60["borough"] == borough].copy()
            base["is_weekend"] = pd.to_datetime(base["date"]).dt.weekday.isin([5, 6])  # <-- fix

            want_wknd = (daytype == "Weekend")
            curve = (
                base[base["is_weekend"] == want_wknd]
                .groupby("hour", as_index=False)["riders"]
                .mean()
            )

            if curve.empty:
                st.warning("No rows for that selection in the last 60 days.")
            else:
                ch = alt.Chart(curve).mark_line(point=True).encode(
                    x=alt.X("hour:O", title="Hour of day"),
                    y=alt.Y("riders:Q", title="Avg riders"),
                    tooltip=["hour","riders"]
                ).properties(height=380)
                plot.altair_chart(ch, use_container_width=True)

    else:  # Rush-hour multiplier
        with area:
            rush = q("""
                select * from vw_rush_hour_multiplier
                where date >= current_date - interval '60 days'
                order by date, borough
            """)
            rush["date"] = to_date(rush["date"])
            sel = st.multiselect(
                "Boroughs",
                ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"],
                default=["Manhattan","Brooklyn","Queens"]
            )
            rr = rush[rush["borough"].isin(sel)].copy()
            if rr.empty:
                st.warning("No rows for selected boroughs.")
            else:
                ch = alt.Chart(rr).mark_line().encode(
                    x="date:T",
                    y=alt.Y("rush_hour_multiplier:Q", title="Peak / Avg"),
                    color="borough:N",
                    tooltip=[
                        "date:T","borough",
                        alt.Tooltip("peak_hourly:Q", title="Peak"),
                        alt.Tooltip("avg_hourly:Q",  title="Avg"),
                        alt.Tooltip("rush_hour_multiplier:Q", title="Peak/Avg")
                    ]
                ).properties(height=380)
                st.altair_chart(ch, use_container_width=True)

# =====================================================================
# WEATHER & EVENTS
# =====================================================================
with tab_weather:
    st.header("Weather & Events")

    # base: joined daily with weather + events (rolling view has flags)
    base = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, coalesce(event_count,0) as event_count
        from vw_ridership_daily_joined
        where date >= current_date - interval '400 days'
        order by date, mode
    """)
    if base.empty:
        st.info("No daily rows available.")
    else:
        base["date"] = to_date(base["date"])

        # --- 1) Ridership vs Temperature (scatter) --------------------
        st.subheader("Ridership vs temperature (last 400 days)")
        mcol, scol = st.columns([1, 4])
        with mcol:
            m = st.radio("Mode", ["subway","bus"], horizontal=True)
        b = base[base["mode"] == m].copy()
        if b.empty:
            st.warning("No points for selection.")
        else:
            scat = alt.Chart(b).mark_point().encode(
                x=alt.X("tmax_f:Q", title="Daily max temperature (°F)"),
                y=alt.Y("riders:Q", title="Riders"),
                color=alt.Color("wet_day:N", title="Wet day"),
                tooltip=["date:T","riders:Q","tmax_f:Q","prcp_in:Q","wet_day:N"]
            ).properties(height=300)
            scol.altair_chart(scat, use_container_width=True)

        st.divider()

        # --- 2) Ridership on event days: boxplot ----------------------
        st.subheader("Event vs non-event days (ridership distribution)")
        bx = base[base["mode"] == m].copy()
        bx["has_event"] = (bx["event_count"].fillna(0) > 0)
        if bx.empty:
            st.warning("No rows for selection.")
        else:
            box = alt.Chart(bx).mark_boxplot(extent="min-max").encode(
                x=alt.X("has_event:N", title="Event day?"),
                y=alt.Y("riders:Q", title=f"{m.title()} riders"),
                color="has_event:N"
            ).properties(height=280, width=300)
            st.altair_chart(box, use_container_width=False)

        st.divider()

        # --- 3) Before/after case study (hourly) ----------------------
        st.subheader("Case study: pick an event date → compare hourly curve vs median")
        # choose dates (2025 only because hourly is 2025+)
        ev = q("""
            select date, sum(event_count)::int as events
            from dim_events_daily
            where date >= '2025-01-01'
            group by date
            having sum(event_count) > 0
            order by events desc, date desc
            limit 200
        """)
        ev["date"] = to_date(ev["date"])
        if ev.empty:
            st.info("No 2025+ event dates loaded yet.")
        else:
            sel_date = st.selectbox(
                "Event date (2025+)",
                options=ev["date"].tolist(),
                format_func=lambda d: f"{d} (events={int(ev.loc[ev['date']==d,'events'].iloc[0])})"
            )

            # Get hourly for chosen date (systemwide = sum across boroughs)
            h = q(f"""
                select date, hour, borough, riders
                from fact_subway_hourly
                where date = '{sel_date}'
                order by hour, borough
            """)
            h["date"] = to_date(h["date"])

            if h.empty:
                st.warning("No hourly subway rows for that date.")
            else:
                day_curve = (
                    h.groupby("hour", as_index=False)["riders"]
                     .sum()
                     .rename(columns={"riders":"riders_event"})
                )

                # 60-day median per hour (excluding that date)
                med = q(f"""
                    select hour, percentile_cont(0.5) within group (order by riders) as med_riders
                    from (
                        select date, hour, sum(riders) as riders
                        from fact_subway_hourly
                        where date >= '{sel_date}'::date - interval '60 days'
                          and date <  '{sel_date}'::date
                        group by date, hour
                    ) t
                    group by hour
                    order by hour
                """)

                comp = day_curve.merge(med, on="hour", how="left")
                if comp.empty:
                    st.warning("No baseline window to compare.")
                else:
                    c1, c2 = st.columns(2)
                    # event day line
                    ch1 = alt.Chart(comp).mark_line(point=True).encode(
                        x=alt.X("hour:O", title="Hour"),
                        y=alt.Y("riders_event:Q", title="Riders"),
                        tooltip=["hour","riders_event"]
                    ).properties(title=f"{sel_date} — event day", height=300)
                    # median line
                    ch2 = alt.Chart(comp).mark_line(point=True).encode(
                        x=alt.X("hour:O", title="Hour"),
                        y=alt.Y("med_riders:Q", title="Riders"),
                        tooltip=["hour","med_riders"]
                    ).properties(title="Typical median (last 60d)", height=300)
                    c1.altair_chart(ch1, use_container_width=True)
                    c2.altair_chart(ch2, use_container_width=True)

# =====================================================================
# DIAGNOSTICS
# =====================================================================
with tab_diags:
    st.subheader("Diagnostics")

    try:
        counts = q("""
            with t1 as (select 'fact_ridership_daily'::text as table, count(*)::bigint as rows from fact_ridership_daily),
                 t2 as (select 'dim_weather_daily', count(*) from dim_weather_daily),
                 t3 as (select 'fact_subway_hourly', count(*) from fact_subway_hourly),
                 t4 as (select 'dim_events_daily',  count(*) from dim_events_daily)
            select * from t1 union all select * from t2 union all select * from t3 union all select * from t4
        """)
        st.dataframe(counts, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning("Counts query failed.")

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
