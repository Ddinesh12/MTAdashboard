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
# TABS
# ---------------------------------------------------------------------
tab_overview, tab_hourly, tab_weather, tab_diags = st.tabs(
    ["Overview", "Hourly patterns", "Weather & Events", "Diagnostics"]
)

# =====================================================================
# OVERVIEW
# =====================================================================
with tab_overview:
    st.header("Daily Ridership and Traffic")

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
    dmin, dmax = daily["date"].min(), daily["date"].max()

    # Right rail filters
    main, rail = st.columns([4, 1])
    with rail:
        st.subheader("Filters")
        mode = st.radio(
            "Mode",
            ["subway", "bus"],
            horizontal=True,
            index=0,
            key="ov_mode",
        )
        drange_dt = st.slider(
            "Date range",
            min_value=pd.to_datetime(dmin),
            max_value=pd.to_datetime(dmax),
            value=(
                pd.to_datetime(max(dmin, last(365))),
                pd.to_datetime(dmax),
            ),
            step=timedelta(days=1),
            format="MMM DD, YYYY",
            key="ov_range",
        )
        drange = (drange_dt[0].date(), drange_dt[1].date())

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
        c1.metric(f"{mode.title()} riders (latest)", f"{last_riders:,}")
        c2.metric("28-day moving avg", f"{int(ma28):,}" if pd.notna(ma28) else "—")
        c3.metric("vs 180-day baseline", f"{pct_vs_180:+.1%}" if pct_vs_180 is not None else "—")

    # Clean line + 7d MA
    base = alt.Chart(df).encode(x="date:T")
    line = base.mark_line().encode(y=alt.Y("riders:Q", title="Riders"))
    ma7  = base.mark_line(strokeDash=[4,3], opacity=0.85).encode(y=alt.Y("riders_ma7:Q", title="7-day MA"))
    main.altair_chart(alt.layer(line, ma7).properties(height=380), use_container_width=True)

# =====================================================================
# HOURLY PATTERNS (two charts visible)
# =====================================================================
with tab_hourly:
    st.header("Subway hourly patterns (2025+)")

    # Load hourly and trim to the true max date we have
    hourly = q("""
        select * from vw_subway_hourly_base
        order by date, hour, borough
    """)
    if hourly.empty:
        st.info("No hourly rows in database yet.")
        st.stop()

    hourly["date"] = to_date(hourly["date"])
    hmax = hourly["date"].max()
    # Only keep last 60 days *available* (so we don't show empty future range)
    hmin = max(hourly["date"].min(), hmax - timedelta(days=59))
    h = hourly[(hourly["date"] >= hmin) & (hourly["date"] <= hmax)].copy()

    # Filters (right rail style but within tab)
    canvas, rail = st.columns([4, 1])
    with rail:
        boro = st.selectbox(
            "Borough (avg by hour)",
            ["All", "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"],
            index=2,
            key="hr_boro",
        )
        daytype = st.radio(
            "Day type",
            ["Weekday", "Weekend"],
            horizontal=True,
            key="hr_daytype",
        )
        sel_weekend = (daytype == "Weekend")

    # Typical day profile (avg hourly curve)
    with canvas:
        st.caption(f"Window shown: {hmin} → {hmax}")
        if boro != "All":
            hh = h[h["borough"] == boro].copy()
        else:
            # Collapse to systemwide by summing boroughs first
            hh = h.groupby(["date", "hour"], as_index=False)["riders"].sum()
            hh["borough"] = "All"

        hh["dow"] = pd.to_datetime(hh["date"]).dt.weekday
        hh["is_weekend"] = hh["dow"].isin([5, 6])

        prof = (
            hh[hh["is_weekend"] == sel_weekend]
            .groupby("hour", as_index=False)["riders"].mean()
        )

        c1, c2 = st.columns(2)

        if prof.empty:
            c1.warning("No rows for this selection in the available window.")
        else:
            ch_prof = alt.Chart(prof).mark_line(point=True).encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("riders:Q", title="Avg riders"),
                tooltip=["hour","riders"]
            ).properties(title=f"Typical day profile — {boro} — {daytype}", height=320)
            c1.altair_chart(ch_prof, use_container_width=True)

        # Rush-hour multiplier (Peak / Avg) by borough
        rush = q(f"""
            with daily as (
              select date, borough,
                     avg(riders)::numeric as avg_hourly,
                     max(riders)          as peak_hourly
              from vw_subway_hourly_base
              where date between '{hmin}' and '{hmax}'
              group by 1,2
            )
            select date, borough,
                   case when avg_hourly = 0 then null else peak_hourly / avg_hourly end as rush_hour_multiplier
            from daily
            order by date, borough
        """)
        rush["date"] = to_date(rush["date"])
        # Let the user pick multiple boroughs, default common trio
        sel_boros = st.multiselect(
            "Rush hour — boroughs",
            ["Bronx","Brooklyn","Manhattan","Queens","Staten Island"],
            default=["Manhattan","Brooklyn","Queens"],
            key="hr_rush_boros",
        )
        rr = rush[rush["borough"].isin(sel_boros)].copy()
        if rr.empty:
            c2.warning("No rows for selected boroughs.")
        else:
            ch_rush = alt.Chart(rr).mark_line().encode(
                x="date:T",
                y=alt.Y("rush_hour_multiplier:Q", title="Peak / Avg"),
                color="borough:N",
                tooltip=["date:T","borough","rush_hour_multiplier:Q"]
            ).properties(title="Rush hour multiplier (last 60 days available)", height=320)
            c2.altair_chart(ch_rush, use_container_width=True)

# =====================================================================
# WEATHER & EVENTS
# =====================================================================
with tab_weather:
    st.header("Weather & Events")

    base = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, coalesce(e.event_count,0) as event_count
        from vw_ridership_daily_joined d
        left join (
          select date, sum(event_count)::int as event_count
          from dim_events_daily
          group by date
        ) e using (date)
        where date >= current_date - interval '400 days'
        order by date, mode
    """)
    if base.empty:
        st.info("No daily rows available.")
    else:
        base["date"] = to_date(base["date"])

        # 1) Ridership vs Temperature
        st.subheader("Ridership vs temperature (last 400 days)")
        mcol, scol = st.columns([1, 4])
        with mcol:
            m = st.radio("Mode", ["subway","bus"], horizontal=True, key="we_mode")
        b = base[base["mode"] == m].copy()
        if b.empty:
            st.warning("No points for selection.")
        else:
            scat = alt.Chart(b).mark_point().encode(
                x=alt.X("tmax_f:Q", title="Daily max temperature (°F)"),
                y=alt.Y("riders:Q", title=f"{m.title()} riders"),
                color=alt.Color("wet_day:N", title="Wet day"),
                tooltip=["date:T","riders:Q","tmax_f:Q","prcp_in:Q","wet_day:N"]
            ).properties(height=300)
            scol.altair_chart(scat, use_container_width=True)

        st.divider()

        # 2) Event vs non-event days — boxplot
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

        # 3) Case study: event day vs 60d median
        st.subheader("Case study: pick an event date → hourly curve vs 60-day median")
        ev = q("""
            select date, sum(event_count)::int as events
            from dim_events_daily
            where date >= '2025-01-01'
            group by date
            having sum(event_count) > 0
            order by events desc, date desc
            limit 200
        """)
        if ev.empty:
            st.info("No 2025+ event dates loaded yet.")
        else:
            ev["date"] = to_date(ev["date"])
            sel_date = st.selectbox(
                "Event date (2025+)",
                options=ev["date"].tolist(),
                format_func=lambda d: f"{d} (events={int(ev.loc[ev['date']==d,'events'].iloc[0])})",
                key="we_case_date",
            )

            h = q(f"""
                select date, hour, riders
                from (
                  select date, hour, sum(riders) as riders
                  from fact_subway_hourly
                  where date = '{sel_date}'
                  group by date, hour
                ) s
                order by hour
            """)
            if h.empty:
                st.warning("No hourly subway rows for that date.")
            else:
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

                left, right = st.columns(2)
                ch1 = alt.Chart(h).mark_line(point=True).encode(
                    x=alt.X("hour:O", title="Hour"),
                    y=alt.Y("riders:Q", title="Riders"),
                    tooltip=["hour","riders"]
                ).properties(title=f"{sel_date} — event day", height=300)
                right_ch = alt.Chart(med).mark_line(point=True).encode(
                    x=alt.X("hour:O", title="Hour"),
                    y=alt.Y("med_riders:Q", title="Riders"),
                    tooltip=["hour","med_riders"]
                ).properties(title="Typical median (last 60d)", height=300)
                left.altair_chart(ch1, use_container_width=True)
                right.altair_chart(right_ch, use_container_width=True)

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
    except Exception:
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
