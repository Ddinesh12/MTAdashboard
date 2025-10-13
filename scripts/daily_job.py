# scripts/daily_job.py
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
import os, sys

from dotenv import load_dotenv
load_dotenv()

ROOT = Path(__file__).resolve().parent.parent if (Path(__file__).parent.name == "scripts") else Path(__file__).parent
SRC  = ROOT / "src"
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
if str(SRC)  not in sys.path: sys.path.insert(0, str(SRC))

from src.extract.mta_daily import fetch_mta_daily
from src.extract.noaa_daily import fetch_noaa_daily
from src.extract.mta_hourly import fetch_mta_hourly_by_borough
from src.extract.events_daily import fetch_events_daily

from src.transform.clean import (
    to_ridership_table,
    to_weather_table,
    to_hourly_table,
    to_events_table,
)

from src.load.to_postgres import upsert_replace_recent_days

TODAY = date.today()

# Refresh windows
DAYS_DAILY  = 7    # ridership + weather
DAYS_HOURLY = 7    # hourly can be revised; keep a week
DAYS_EVENTS = 14   # events can trickle in late

def daily_refresh():
    s_daily = (TODAY - timedelta(days=DAYS_DAILY)).isoformat()
    s_hour  = (TODAY - timedelta(days=DAYS_HOURLY)).isoformat()
    s_ev    = (TODAY - timedelta(days=DAYS_EVENTS)).isoformat()
    e       = TODAY.isoformat()

    # Daily ridership + weather
    mta = to_ridership_table(fetch_mta_daily(s_daily, e))
    wx  = to_weather_table(fetch_noaa_daily(s_daily, e))
    upsert_replace_recent_days(mta, "fact_ridership_daily", date_col="date", days=DAYS_DAILY,  pkey=["date","mode"])
    upsert_replace_recent_days(wx,  "dim_weather_daily",    date_col="date", days=DAYS_DAILY,  pkey="date")
    print(f"[daily] refreshed last {DAYS_DAILY} days → r={len(mta)} w={len(wx)}")

    # Hourly (2025+)
    hh = to_hourly_table(fetch_mta_hourly_by_borough(s_hour, e))
    upsert_replace_recent_days(hh, "fact_subway_hourly", date_col="date", days=DAYS_HOURLY, pkey=["date","hour","borough"])
    print(f"[hourly] refreshed last {DAYS_HOURLY} days → h={len(hh)}")

    # Events
    ev = to_events_table(fetch_events_daily(s_ev, e))
    upsert_replace_recent_days(ev, "dim_events_daily", date_col="date", days=DAYS_EVENTS, pkey=["date","borough"])
    print(f"[events] refreshed last {DAYS_EVENTS} days → e={len(ev)}")

if __name__ == "__main__":
    print("=== DAILY JOB START ===")
    daily_refresh()
    print("=== DAILY JOB COMPLETE ===")
