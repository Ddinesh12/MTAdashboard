import os
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

# ---------- setup ----------
load_dotenv()
engine = sa.create_engine(os.environ["NEON_DATABASE_URL"], pool_pre_ping=True)
st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")

@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    """Fast path: cached query helper."""
    with engine.connect() as c:
        return pd.read_sql(sql, c)

def q_safe(sql: str) -> pd.DataFrame:
    """Same as q() but shows a user-friendly error and returns empty df on failure."""
    try:
        return q(sql)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

st.title("NYC MTA Dashboard")

# ---------- Diagnostics / sanity checks ----------
with st.expander("🔍 Diagnostics (row counts & sample rows)"):
    counts = q_safe("""
        select * from (
            select 'fact_ridership_daily'::text as t, count(*)::bigint as n from fact_ridership_daily
            union all select 'dim_weather_daily',  count(*) from dim_weather_daily
            union all select 'fact_subway_hourly', count(*) from fact_subway_hourly
            union all select 'dim_events_daily',   count(*) from dim_events_daily
        ) s
    """)
    if not counts.empty:
        c1, c2, c3, c4 = st.columns(4)
        for col, row in zip([c1, c2, c3, c4], counts.itertuples(index=False)):
            with col:
                st.metric(row.t, int(row.n))

    st.caption("Preview a few recent rows from each table:")

    preview_sql = {
        "fact_ridership_daily": """
            select * from fact_ridership_daily
            order by date desc, mode asc
            limit 10
        """,
        "dim_weather_daily": """
            select * from dim_weather_daily
            order by date desc
            limit 10
        """,
        "fact_subway_hourly": """
            select * from fact_subway_hourly
            order by date desc, hour desc, borough asc
            limit 10
        """,
        "dim_events_daily": """
            select * from dim_events_daily
            order by date desc, borough asc
            limit 10
        """,
    }
    for name, sql in preview_sql.items():
        st.markdown(f"**{name}**")
        df_prev = q_safe(sql)
        if df_prev.empty:
            st.info("no rows")
        else:
            st.dataframe(df_prev, use_container_width=True)

# ---------- Main tabs ----------
tab1, tab2, tab3 = st.tabs(["Daily", "Hourly (2025+)", "Events"])

with tab1:
    df = q_safe("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
        from vw_ridership_daily_joined
        where date >= current_date - interval '365 days'
        order by date, mode
    """)
    if df.empty:
        st.warning("No daily ridership rows returned for the last 365 days.")
    else:
        st.subheader("Daily ridership (systemwide)")
        chart_df = df.pivot(index="date", columns="mode", values="riders")
        st.line_chart(chart_df, use_container_width=True)
        with st.expander("Data"):
            st.dataframe(df, use_container_width=True)

with tab2:
    hh = q_safe("""
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
    """)
    if hh.empty:
        st.info("No hourly rows in the last 60 days (remember: we only loaded 2025+ for hourly).")
    else:
        st.subheader("Hourly subway ridership (last 60 days)")
        st.dataframe(hh.head(500), use_container_width=True)
        st.caption("Showing first 500 rows for brevity. Use SQL views to slice further.")

with tab3:
    ev = q_safe("""
        select date, sum(event_count)::int as events
        from dim_events_daily
        group by date
        order by date desc
        limit 365
    """)
    if ev.empty:
        st.info("No events data found yet in dim_events_daily.")
    else:
        st.subheader("Permitted events per day (last 365 loaded days)")
        st.bar_chart(ev.set_index("date").sort_index(), use_container_width=True)
        with st.expander("Data"):
            st.dataframe(ev.sort_values("date", ascending=False), use_container_width=True)
