# app.py
import os
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from dotenv import load_dotenv

# --- setup --------------------------------------------------------------------
load_dotenv()  # Streamlit Cloud uses st.secrets; local dev can use a .env
DB_URL = os.getenv("NEON_DATABASE_URL", st.secrets.get("NEON_DATABASE_URL", ""))

if not DB_URL:
    st.error("NEON_DATABASE_URL is not set. Add it to Streamlit secrets or a local .env.")
    st.stop()

engine = sa.create_engine(DB_URL, pool_pre_ping=True)

st.set_page_config(page_title="NYC MTA Dashboard", layout="wide")
st.title("NYC MTA Dashboard")

# --- helpers ------------------------------------------------------------------
@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    """Plain query helper (raises on error)."""
    with engine.connect() as c:
        return pd.read_sql(sql, c)

@st.cache_data(ttl=600)
def q_safe(sql: str) -> pd.DataFrame:
    """Query helper that never crashes the app — shows a compact error instead."""
    try:
        return q(sql)
    except Exception as e:
        st.warning(f"Query failed: {getattr(e, 'orig', e)}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def table_date_range(table: str) -> pd.DataFrame:
    """min/max/count for a table that has a 'date' column."""
    sql = f"select min(date) as min_d, max(date) as max_d, count(*)::bigint as n from {table}"
    return q_safe(sql)

def window_note(df: pd.DataFrame, table: str, intended: str) -> None:
    rng = table_date_range(table)
    if not rng.empty:
        min_d, max_d, n = rng.loc[0, ["min_d", "max_d", "n"]]
        st.caption(
            f"Showing **{len(df):,}** rows. Intended window: *{intended}*. "
            f"Available in `{table}`: **{min_d} → {max_d}** ({int(n):,} rows)."
        )

# --- quick diagnostics row ----------------------------------------------------
st.subheader("Diagnostics")
counts = q_safe(
    """
    select 'fact_ridership_daily'::text as table, count(*)::bigint as rows from fact_ridership_daily
    union all select 'dim_weather_daily',  count(*) from dim_weather_daily
    union all select 'fact_subway_hourly', count(*) from fact_subway_hourly
    union all select 'dim_events_daily',   count(*) from dim_events_daily
    order by table
    """
)
st.dataframe(counts, use_container_width=True, hide_index=True)

cols = st.columns(4)
with cols[0]:
    st.caption("ridership_daily (latest 5)")
    st.dataframe(
        q_safe("""select * from fact_ridership_daily order by date desc, mode asc limit 5"""),
        use_container_width=True, hide_index=True
    )
with cols[1]:
    st.caption("weather_daily (latest 5)")
    st.dataframe(
        q_safe("""select * from dim_weather_daily order by date desc limit 5"""),
        use_container_width=True, hide_index=True
    )
with cols[2]:
    st.caption("subway_hourly (latest 5)")
    st.dataframe(
        q_safe("""select * from fact_subway_hourly order by date desc, hour desc, borough asc limit 5"""),
        use_container_width=True, hide_index=True
    )
with cols[3]:
    st.caption("events_daily (latest 5)")
    st.dataframe(
        q_safe("""select * from dim_events_daily order by date desc, borough asc limit 5"""),
        use_container_width=True, hide_index=True
    )

st.divider()

# --- tabs ---------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Daily", "Hourly (2025+)", "Events"])

# ----- Daily ------------------------------------------------------------------
with tab1:
    intended = "last 365 days"
    df = q_safe(
        """
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
        from vw_ridership_daily_joined
        where date >= current_date - interval '365 days'
        order by date, mode
        """
    )
    if df.empty:
        # fallback: show whatever is available so charts aren’t blank
        df = q_safe(
            """
            select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
            from vw_ridership_daily_joined
            order by date, mode
            """
        )
        if not df.empty:
            st.info("No rows in the last 365 days — showing **all available** daily data instead.")
    if df.empty:
        st.warning("No daily ridership data is loaded yet.")
    else:
        st.subheader("Daily ridership (systemwide)")
        st.line_chart(
            df.pivot(index="date", columns="mode", values="riders"),
            use_container_width=True
        )
        window_note(df, "fact_ridership_daily", intended)
        with st.expander("Data"):
            st.dataframe(df, use_container_width=True, hide_index=True)

# ----- Hourly -----------------------------------------------------------------
with tab2:
    intended = "last 60 days"
    hh = q_safe(
        """
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
        """
    )
    if hh.empty:
        hh = q_safe(
            """
            select date, hour, borough, riders
            from vw_subway_hourly_base
            order by date, hour, borough
            """
        )
        if not hh.empty:
            st.info("No rows in the last 60 days — showing **all available** hourly data instead.")
    if hh.empty:
        st.warning("No hourly data is loaded yet.")
    else:
        st.subheader("Hourly subway ridership")
        # For now, keep it simple; you can upgrade this to charts later
        st.dataframe(hh.head(1000), use_container_width=True, hide_index=True)
        window_note(hh, "fact_subway_hourly", intended)

# ----- Events -----------------------------------------------------------------
with tab3:
    intended = "last 365 loaded days (most recent first)"
    ev = q_safe(
        """
        select date, sum(event_count)::int as events
        from dim_events_daily
        group by date
        order by date desc
        limit 365
        """
    )
    if ev.empty:
        ev = q_safe(
            """
            select date, sum(event_count)::int as events
            from dim_events_daily
            group by date
            order by date
            """
        )
        if not ev.empty:
            st.info("No recent events — showing **all available** events instead.")
    if ev.empty:
        st.warning("No events data is loaded yet.")
    else:
        st.subheader("Permitted events per day")
        st.bar_chart(ev.set_index("date").sort_index(), use_container_width=True)
        window_note(ev, "dim_events_daily", intended)
        with st.expander("Data"):
            st.dataframe(ev.sort_values("date", ascending=False), use_container_width=True, hide_index=True)
