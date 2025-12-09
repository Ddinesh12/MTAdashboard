# app.py
import os
from datetime import date, datetime, timedelta

import pandas as pd
import numpy as np
import sqlalchemy as sa
import streamlit as st
import altair as alt
import requests
from dotenv import load_dotenv

# -----------------------------
# Setup
# -----------------------------
# Load environment variables from .env
load_dotenv()

# Create a SQLAlchemy engine for Neon Postgres (URL stored in NEON_DATABASE_URL)
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

# Optional Socrata app token (for NYC Open Data)
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

# Configure Streamlit page layout and title
st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")


@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    """
    Simple query helper:
      - runs a SQL string against the Neon database
      - returns the result as a pandas DataFrame
      - cached for 600 seconds to avoid hitting the DB too often
    """
    with engine.connect() as c:
        return pd.read_sql(sql, c)


def pct(a, b):
    """
    Compute percent difference (a - b) / b, guarding against zero/None/NaN.
    Returns None when b is zero or missing.
    """
    if b in (0, None) or pd.isna(b):
        return None
    return (a - b) / b


def to_pydate(d):
    """
    Convert a date-like value into a Python datetime object.
    Used for Streamlit sliders (which work with datetime, not raw strings).
    """
    return pd.to_datetime(d).to_pydatetime()


# -----------------------------
# Sidebar (left nav)
# -----------------------------
st.sidebar.header("Pages")

# Simple radio-based navigation between logical pages
page = st.sidebar.radio(
    "Search for pages on the website",
    ["Overview", "Hourly Patterns", "Weather & Events", "Diagnostics"],
    label_visibility="collapsed",
    key="nav_page",
)

# Small CSS tweaks to make sidebar wider and metric text larger
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

    # Pull daily ridership with rolling metrics and weather fields
    daily = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day,
               riders_ma7, riders_ma28, riders_baseline_180
        from vw_ridership_daily_rolling
        order by date, mode
    """)

    if daily.empty:
        st.warning("No daily data available yet.")
        st.stop()

    # Determine full date range from data for slider bounds
    dmin = daily["date"].min()
    dmax = daily["date"].max()
    min_dt = to_pydate(dmin)
    max_dt = to_pydate(dmax)
    default_start = max_dt - timedelta(days=365)  # default view: last ~12 months

    st.caption("Select a start and end date")
    # Date range slider
    drange_dt = st.slider(
        "Date range",
        min_value=min_dt,
        max_value=max_dt,
        value=(max(min_dt, default_start), max_dt),
        step=timedelta(days=1),
        format="MMM DD, YYYY",
        key="ov_range",
        label_visibility="collapsed",
    )
    drange = (drange_dt[0].date(), drange_dt[1].date())

    # Choose mode (subway vs bus)
    mode = st.radio("Mode", ["subway", "bus"], horizontal=True, key="ov_mode")

    # Filter data to selected range + mode
    df = daily[
        (daily["date"] >= drange[0])
        & (daily["date"] <= drange[1])
        & (daily["mode"] == mode)
    ]

    # Top-level KPIs in three columns
    colA, colB, colC = st.columns(3)
    last_day = df["date"].max() if not df.empty else None
    last_row = df.loc[df["date"] == last_day].head(1) if last_day else pd.DataFrame()

    # Show metrics for the latest day in the selected range
    if not last_row.empty:
        lr = last_row.iloc[0]
        # Current riders
        colA.metric(f"{mode.title()} riders (last day)", f"{int(lr['riders']):,}")

        # Compare vs 28-day moving average
        if not pd.isna(lr["riders_ma28"]):
            delta_vs_ma28 = pct(lr["riders"], lr["riders_ma28"])
            colB.metric(
                "vs. 28-day MA",
                f"{lr['riders_ma28']:.0f}",
                f"{delta_vs_ma28*100:+.1f}%"
            )
        else:
            colB.metric("vs. 28-day MA", "—")

        # Compare vs 180-day baseline
        if not pd.isna(lr["riders_baseline_180"]):
            delta_vs_180 = pct(lr["riders"], lr["riders_baseline_180"])
            colC.metric(
                "vs. 180-day baseline",
                f"{lr['riders_baseline_180']:.0f}",
                f"{delta_vs_180*100:+.1f}%"
            )
        else:
            colC.metric("vs. 180-day baseline", "—")
    else:
        # No data in range; show placeholders
        colA.metric(f"{mode.title()} riders (last day)", "—")
        colB.metric("vs. 28-day MA", "—")
        colC.metric("vs. 180-day baseline", "—")

    # Main time-series chart
    st.subheader("Daily trend")
    if df.empty:
        st.info("No rows in the selected window. Try widening the date range.")
    else:
        st.line_chart(df[["date", "riders"]].set_index("date"))

    # Expandable raw data view
    with st.expander("Show data"):
        st.dataframe(
            df[
                [
                    "date",
                    "mode",
                    "riders",
                    "wet_day",
                    "hot_day",
                    "cold_day",
                    "riders_ma7",
                    "riders_ma28",
                    "riders_baseline_180",
                ]
            ].sort_values("date"),
            use_container_width=True,
        )

# =========================================================
# 2) HOURLY PATTERNS (2025+)
# =========================================================
elif page == "Hourly Patterns":
    st.title("Hourly Patterns (Subway, 2025+)")

    # Last 60 days of hourly subway data (already filtered in the view)
    base = q("""
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
    """)
    if base.empty:
        st.warning("No hourly rows in the last 60 days (remember: we’ve only loaded 2025+).")
        st.stop()

    # Add weekend/weekday flag for filtering
    base["date"] = pd.to_datetime(base["date"])
    base["is_weekend"] = base["date"].dt.dayofweek.isin([5, 6])

    # Dropdown list for borough selection
    all_boros = ["All boroughs", "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]

    c1, c2, _ = st.columns([1, 1, 2])
    with c1:
        part = st.radio("Day type", ["Weekday", "Weekend"], horizontal=True, key="hp_daytype")
    with c2:
        sel_boro = st.selectbox("Borough", all_boros, key="hp_boro")

    # Filter by weekday/weekend
    if part == "Weekend":
        sub = base[base["is_weekend"]]
    else:
        sub = base[~base["is_weekend"]]

    # Filter by borough (or use all)
    if sel_boro != "All boroughs":
        sub = sub[sub["borough"] == sel_boro]

    st.subheader("Typical day profile")
    if sub.empty:
        st.info("No rows for that selection in the last 60 days.")
    else:
        # Average riders per hour, across the filtered days
        prof = sub.groupby("hour", as_index=False)["riders"].mean().sort_values("hour")
        st.line_chart(prof.set_index("hour"))

    # Rush-hour multiplier time series (peak / average hourly)
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
            # Show multiple lines, one per borough
            piv = rh.pivot_table(index="date", columns="borough", values="rush_hour_multiplier")
            st.line_chart(piv)
        else:
            # Show single line for selected borough
            one = rh[rh["borough"] == sel_boro][["date", "rush_hour_multiplier"]].set_index("date")
            if one.empty:
                st.info("No rows for that borough in the last 60 days.")
            else:
                st.line_chart(one)

    # Quick sample of raw hourly rows
    with st.expander("Show hourly sample"):
        st.dataframe(base.tail(200), use_container_width=True)

# =========================================================
# 3) WEATHER & EVENTS
# =========================================================
elif page == "Weather & Events":
    st.title("Weather & Events")

    # ---- RIDERSHIP vs TEMPERATURE (last 400d)
    st.subheader("Ridership vs Temperature (last 400 days)")
    mode = st.radio("Mode", ["subway", "bus"], horizontal=True, key="wx_mode")

    # Daily ridership joined with weather + events
    daily = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
        from vw_ridership_daily_joined
        where date >= (current_date - interval '400 days')
        order by date, mode
    """)
    d = daily[daily["mode"] == mode].copy()

    # Ensure numeric types for plotting
    d["tmax_f"] = pd.to_numeric(d["tmax_f"], errors="coerce")
    d["riders"] = pd.to_numeric(d["riders"], errors="coerce")
    d = d.dropna(subset=["tmax_f", "riders"])

    # Label each day as Wet or Dry for chart coloring
    d["wet_label"] = np.where(d["wet_day"] == True, "Wet day", "Dry day")

    if d.empty:
        st.info("No daily rows available yet.")
    else:
        # Scatter plot: ridership vs temperature, color by wet/dry
        scatter = (
            alt.Chart(d)
            .mark_circle(opacity=0.65)
            .encode(
                x=alt.X("tmax_f:Q", title="Max temperature (°F)"),
                y=alt.Y("riders:Q", title=f"{mode.title()} riders"),
                color=alt.Color("wet_label:N", title=""),
                tooltip=["date:T", "riders:Q", "tmax_f:Q", "wet_label:N", "event_count:Q"],
            )
            .properties(height=360)
        )
        st.altair_chart(scatter, use_container_width=True)

    # ---- EVENT DAY EFFECT (boxplot)
    st.subheader("Ridership on event days (last 400 days)")
    if d.empty:
        st.info("No data for boxplot.")
    else:
        d2 = d.copy()
        # Flag days with at least one event vs zero events
        d2["has_event"] = np.where(d2["event_count"].fillna(0) > 0, "≥1 event", "0 events")
        box = (
            alt.Chart(d2)
            .mark_boxplot(extent="min-max")
            .encode(
                x=alt.X("has_event:N", title="Event day"),
                y=alt.Y("riders:Q", title=f"{mode.title()} riders"),
                color=alt.Color("has_event:N", title=""),
                tooltip=["has_event:N", "riders:Q"],
            )
            .properties(height=320)
        )
        st.altair_chart(box, use_container_width=True)

    # ---- BEFORE/AFTER CASE STUDY (with on-demand event list)
    st.subheader("Case study: high-event day vs typical profile (subway)")

    # Get top event dates (systemwide sums) in the last 180 days
    top_events = q("""
        select date, sum(event_count)::int as events
        from dim_events_daily
        where date >= (current_date - interval '180 days')
        group by date
        having sum(event_count) > 0
        order by events desc, date desc
        limit 30
    """)
    if top_events.empty:
        st.info("No event counts available in the last 180 days.")
    else:
        # Build labels like "2025-05-01  (events: 12)"
        sel_label = st.selectbox(
            "Pick a high-event date",
            [f"{r.date}  (events: {r.events})" for _, r in top_events.iterrows()],
            index=0,
            key="cs_pick",
        )
        sel_date = pd.to_datetime(sel_label.split()[0]).date()

        # Hourly ridership on the selected date (sum later across boroughs)
        hr_day = q(f"""
            select date, hour, borough, riders
            from fact_subway_hourly
            where date = '{sel_date}'
            order by hour, borough
        """)

        # Baseline: hours on same weekday over previous 60 days
        baseline_window_start = sel_date - timedelta(days=60)
        hr_base = q(f"""
            select date, hour, borough, riders
            from vw_subway_hourly_base
            where date between '{baseline_window_start}' and '{sel_date - timedelta(days=1)}'
            order by date, hour, borough
        """)

        col1, col2 = st.columns(2)

        if hr_day.empty:
            col1.info("No hourly data for the selected day.")
        else:
            # Sum riders across boroughs for the selected day by hour
            h1 = (
                hr_day.groupby("hour", as_index=False)["riders"].sum()
                .rename(columns={"riders": "riders_day"})
            )

            # Build baseline median profile for the same weekday
            baseline = None
            if not hr_base.empty:
                hr_base["date"] = pd.to_datetime(hr_base["date"])
                target_dow = pd.Timestamp(sel_date).dayofweek
                # Only days with same weekday as selected date
                same_dow = hr_base[hr_base["date"].dt.dayofweek == target_dow]
                if not same_dow.empty:
                    # Sum per day+hour so we can take median across days
                    daily_totals = same_dow.groupby(["date", "hour"], as_index=False)["riders"].sum()
                    baseline = (
                        daily_totals.groupby("hour", as_index=False)["riders"].median()
                        .rename(columns={"riders": "riders_median"})
                    )

            # Merge day profile with baseline median
            if baseline is not None:
                merged = pd.merge(h1, baseline, on="hour", how="outer").sort_values("hour")
            else:
                merged = h1.copy()
                merged["riders_median"] = pd.NA

            # Melt into long format for Altair (series=day vs median)
            melted = merged.melt("hour", var_name="series", value_name="riders")
            line = (
                alt.Chart(melted)
                .mark_line(point=True)
                .encode(
                    x=alt.X("hour:O", title="Hour of day"),
                    y=alt.Y("riders:Q", title="Riders (sum across boroughs)"),
                    color=alt.Color("series:N", title=None),
                    tooltip=["hour:O", "series:N", "riders:Q"],
                )
                .properties(height=350)
            )
            col1.altair_chart(line, use_container_width=True)

            with col2:
                st.caption(
                    f"Selected date: **{sel_date}** — day vs. median of same weekday (prior 60 days)"
                )
                st.dataframe(merged, use_container_width=True)

        # --- On-demand event list for the selected date (live from NYC Open Data)
        st.subheader("Events on selected date (on-demand from NYC Open Data)")
        with st.spinner("Fetching events…"):
            ev_df = None
            try:
                ev_df = fetch_events_for_date(sel_date, SOCRATA_TOKEN)
            except Exception as ex:
                st.error(f"Failed to fetch events: {ex}")

        if ev_df is None or ev_df.empty:
            st.info("No events found for that date in the source dataset.")
        else:
            # Show selected columns for readability
            show = ev_df[
                [
                    "event_name",
                    "event_type",
                    "event_borough",
                    "event_location",
                    "start_date_time",
                    "end_date_time",
                    "street_closure_type",
                    "community_board",
                    "police_precinct",
                ]
            ].copy()
            st.dataframe(show, use_container_width=True)

    # Small raw daily sample at the bottom
    with st.expander("Raw daily rows (last 14)"):
        st.dataframe(
            q("""
                select date, mode, riders, tmax_f, prcp_in, wet_day, event_count
                from vw_ridership_daily_joined
                order by date desc, mode
                limit 28
            """),
            use_container_width=True,
        )

# =========================================================
# 4) DIAGNOSTICS
# =========================================================
else:
    st.title("Diagnostics")

    # Simple table row counts per core table
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

    # Preview latest few rows from each core table
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


# -----------------------------
# Helper: on-demand events fetch
# -----------------------------
def fetch_events_for_date(the_date: date, token: str | None) -> pd.DataFrame:
    """
    Fetch detailed event records for a given calendar date
    from NYC Open Data dataset `tvpp-9vvx` (NYC Permitted Event Information).

    Includes events that:
      - start that day,
      - end that day,
      - or span across that day.
    """
    base = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"
    start_iso = f"{the_date}T00:00:00"
    end_iso   = f"{the_date}T23:59:59"

    # WHERE condition to capture events overlapping the given day
    where = (
        f"(start_date_time between '{start_iso}' and '{end_iso}') OR "
        f"(end_date_time between '{start_iso}' and '{end_iso}') OR "
        f"(start_date_time <= '{end_iso}' AND end_date_time >= '{start_iso}')"
    )

    # Limit fields to only whats needed for display
    params = {
        "$select": "event_id,event_name,event_type,event_borough,event_location,street_closure_type,"
                   "community_board,police_precinct,start_date_time,end_date_time",
        "$where": where,
        "$limit": 5000,
        "$order": "start_date_time"
    }

    headers = {"User-Agent": "nyc-mta-dashboard/0.1"}
    if token:
        headers["X-App-Token"] = token

    # Call the API and error if something goes wrong
    r = requests.get(base, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(rows)

    # Best-effort datetime parsing for start/end fields
    for c in ["start_date_time", "end_date_time"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    return df
