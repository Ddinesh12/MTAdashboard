import os
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

tab1, tab2, tab3 = st.tabs(["Daily", "Hourly (2025+)", "Events"])

with tab1:
    df = q("""
        select date, mode, riders, tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
        from vw_ridership_daily_joined
        where date >= current_date - interval '365 days'
        order by date, mode
    """)
    st.line_chart(df.pivot(index="date", columns="mode", values="riders"))

with tab2:
    hh = q("""
        select date, hour, borough, riders
        from vw_hourly_last60
        order by date, hour, borough
    """)
    st.dataframe(hh.head(200))  # quick placeholder; replace with charts later

with tab3:
    ev = q("""
        select date, sum(event_count)::int as events
        from dim_events_daily
        group by date
        order by date desc
        limit 365
    """)
    st.bar_chart(ev.set_index("date"))
