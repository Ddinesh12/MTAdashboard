# app.py
import os
from datetime import date, timedelta

import altair as alt
import numpy as np
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

# ---------------------------
# Setup
# ---------------------------
load_dotenv()
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)

st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")

@st.cache_data(ttl=600, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    with engine.connect() as c:
        return pd.read_sql(sql, c)

def _fmt_delta(pct: float | None) -> str:
    if pct is None or pd.isna(pct):
        return "—"
    return f"{pct*100:+.1f}%"

# ---------------------------
# Sidebar (global)
# ---------------------------
with st.sidebar:
    st.markdown("### Filters")
    # 12 months default window for Overview
    end_d = pd.to_datetime(date.today())
    start_d = end_d - pd.Timedelta(days=365)
    dr = st.date_input(
        "Overview date range",
        value=(start_d.date(), end_d.date()),
        help="Used for the Overview charts",
    )
    show_weather = st.checkbox("Show weather markers", value=True)
    show_events = st.checkbox("Show event markers", value=True)

    st.markdown("---")
    st.markdown("### Hourly filters")
    borough = st.selectbox(
        "Borough",
        ["All", "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"],
        index=0,
    )
    daytype = st.radio("Day type (typical curve)", ["Weekday", "Weekend"], horizontal=True)

st.title("NYC MTA Dashboard")

tab1, tab2 = st.tabs(["Overview", "Hourly (2025+)"])

# =========================================================
# TAB 1 — OVERVIEW
# =========================================================
with tab1:
    # ---------- Pull data ----------
    df_roll = q("""
        select date, mode, riders, riders_ma7, riders_ma28, riders_baseline_180
        from vw_ridership_daily_rolling
        order by date, mode
    """)
    df_join = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
        from vw_ridership_daily_joined
        order by date, mode
    """)

    # Date filter for overview range
    if isinstance(dr, tuple) and len(dr) == 2:
        d0, d1 = [pd.to_datetime(x).date() for x in dr]
        mask = (pd.to_datetime(df_roll["date"]).dt.date >= d0) & (pd.to_datetime(df_roll["date"]).dt.date <= d1)
        df_roll_f = df_roll.loc[mask].copy()
        mask2 = (pd.to_datetime(df_join["date"]).dt.date >= d0) & (pd.to_datetime(df_join["date"]).dt.date <= d1)
        df_join_f = df_join.loc[mask2].copy()
    else:
        df_roll_f = df_roll.copy()
        df_join_f = df_join.copy()

    # ---------- KPI cards (last non-null day per mode) ----------
    k1, k2, k3 = st.columns(3)
    def _kpi_for_mode(mode: str, col):
        dmode = df_roll.dropna(subset=["riders"]).query("mode == @mode").sort_values("date")
        if dmode.empty:
            col.metric(mode.capitalize(), value="—", delta="—")
            return
        last = dmode.iloc[-1]
        v = int(last["riders"]) if not pd.isna(last["riders"]) else None
        ma28 = last.get("riders_ma28", np.nan)
        base180 = last.get("riders_baseline_180", np.nan)

        # deltas
        delta_vs_ma = None if pd.isna(ma28) or ma28 == 0 else (v - ma28) / ma28
        delta_vs_base = None if pd.isna(base180) or base180 == 0 else (v - base180) / base180

        title = f"{mode.capitalize()} (last day)"
        sub = f"vs 28d: {_fmt_delta(delta_vs_ma)} | vs 180d: {_fmt_delta(delta_vs_base)}"
        col.metric(title, f"{v:,}" if v is not None else "—", sub)

    _kpi_for_mode("subway", k1)
    _kpi_for_mode("bus", k2)

    # headroom / spacing card
    with k3:
        st.caption("Latest data reflects the most recent publishing day for each mode.")

    # ---------- Daily trend (12 months window) ----------
    st.markdown("### Daily ridership (systemwide)")
    if df_join_f.empty:
        st.info("No daily rows in the selected window.")
    else:
        # Altair line by mode
        base = alt.Chart(df_join_f).encode(
            x=alt.X("date:T", title="", axis=alt.Axis(format="%b %d", labelOverlap=True)),
        )

        line = base.mark_line().encode(
            y=alt.Y("riders:Q", title="Riders"),
            color=alt.Color("mode:N", title="Mode"),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("mode:N", title="Mode"),
                alt.Tooltip("riders:Q", title="Riders", format=",.0f"),
            ],
        )

        ma7 = base.transform_window(
            ma7='mean(riders)', frame=[-6, 0], groupby=["mode"]
        ).mark_line(opacity=0.5, strokeDash=[4,2]).encode(
            y='ma7:Q', color='mode:N'
        )

        layers = [line, ma7]

        # Optional weather markers
        if show_weather:
            wet_pts = base.transform_filter(alt.datum.wet_day == True).mark_point(size=30, opacity=0.4).encode(
                y="riders:Q",
                shape=alt.value("circle"),
                color=alt.value("#1f77b4"),
                tooltip=["date:T","mode:N","riders:Q","tmax_f:Q","prcp_in:Q"]
            )
            layers.append(wet_pts)

        # Optional event markers (small lollipops at riders value, only when >0 events)
        if show_events:
            ev = df_join_f.copy()
            ev["has_event"] = ev["event_count"].fillna(0) > 0
            evc = alt.Chart(ev).transform_filter(alt.datum.has_event == True)
            stem = evc.mark_rule(opacity=0.35).encode(
                x="date:T", y="riders:Q"
            )
            dot = evc.mark_point(size=35, filled=True, opacity=0.5, color="#e45756").encode(
                x="date:T", y="riders:Q",
                tooltip=["date:T","event_count:Q"]
            )
            layers += [stem, dot]

        chart = alt.layer(*layers).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    # ---------- Data table (optional) ----------
    with st.expander("Data (Overview)"):
        st.dataframe(df_join_f.sort_values(["date","mode"]).reset_index(drop=True))


# =========================================================
# TAB 2 — HOURLY (2025+)
# =========================================================
with tab2:
    st.markdown("#### Hourly heatmap (last 60 days)")

    hh = q("""
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
    """)
    if borough != "All":
        hh = hh.query("borough == @borough")

    if hh.empty:
        st.info("No hourly rows in the last 60 days for the selected filters.")
    else:
        # Build a (date × hour) matrix (sum across borough if 'All')
        if borough == "All":
            mat = (hh.groupby(["date","hour"], as_index=False)["riders"].sum())
        else:
            mat = hh.copy()

        heat = alt.Chart(mat).mark_rect().encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y("date:T", title="Date", sort="descending"),
            color=alt.Color("riders:Q", title="Riders", scale=alt.Scale(type="linear")),
            tooltip=["date:T","hour:O","riders:Q"]
        ).properties(height=360)
        st.altair_chart(heat, use_container_width=True)

    st.markdown("#### Typical day curve")
    curves = q("""
        select borough, hour, weekend_factor, weekend_avg, weekday_avg
        from vw_weekend_factor
        order by borough, hour
    """)
    if borough != "All":
        curves = curves.query("borough == @borough")
    else:
        # average across boroughs for the “All” view
        curves = (curves.groupby("hour", as_index=False)
                        .agg(weekend_avg=("weekend_avg","mean"),
                             weekday_avg=("weekday_avg","mean"),
                             weekend_factor=("weekend_factor","mean")))
        curves["borough"] = "All"

    if curves.empty:
        st.info("No curves available.")
    else:
        hm = "weekend_avg" if daytype == "Weekend" else "weekday_avg"
        cchart = alt.Chart(curves).mark_line(point=True).encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y(f"{hm}:Q", title="Riders (avg)"),
            color=alt.value("#1f77b4"),
            tooltip=["hour:O", alt.Tooltip(f"{hm}:Q", title="Riders", format=",.0f")]
        ).properties(height=250)
        st.altair_chart(cchart, use_container_width=True)

    st.markdown("#### Rush-hour multiplier (peak / avg hourly)")
    rush = q("""
        select date, borough, rush_hour_multiplier, peak_hourly, avg_hourly
        from vw_rush_hour_multiplier
        order by date, borough
    """)
    if borough != "All":
        rush = rush.query("borough == @borough")

    if rush.empty:
        st.info("No rush-hour multiplier rows.")
    else:
        rchart = alt.Chart(rush).mark_line().encode(
            x=alt.X("date:T", title=""),
            y=alt.Y("rush_hour_multiplier:Q", title="Multiplier"),
            color=alt.Color("borough:N", title="Borough"),
            tooltip=["date:T","borough:N", alt.Tooltip("rush_hour_multiplier:Q", format=".2f")]
        ).properties(height=260)
        st.altair_chart(rchart, use_container_width=True)

    with st.expander("Data (Hourly)"):
        st.dataframe(hh.head(500))
