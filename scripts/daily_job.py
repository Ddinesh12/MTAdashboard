# scripts/daily_job.py
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
import os, sys

from dotenv import load_dotenv
load_dotenv()  # Load environment variables (DB URL, tokens, etc.) from .env

# -------------------------------------------------
# Figure out project root and ensure src/ is importable
# -------------------------------------------------
# If this file lives in scripts/, ROOT is the project root (one level up),
# otherwise ROOT is just the current directory.
ROOT = (
    Path(__file__).resolve().parent.parent
    if (Path(__file__).parent.name == "scripts")
    else Path(__file__).parent
)
SRC = ROOT / "src"

# Add ROOT and SRC to sys.path so "src.*" imports work when running as a script
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# -------------------------------
# Extractors: pull raw data from APIs
# -------------------------------
from src.extract.mta_daily import fetch_mta_daily
from src.extract.noaa_daily import fetch_noaa_daily
from src.extract.mta_hourly import fetch_mta_hourly_by_borough
from src.extract.events_daily import fetch_events_daily

# -------------------------------
# Transformers: clean/normalize data into table-ready shapes
# -------------------------------
from src.transform.clean import (
    to_ridership_table,
    to_weather_table,
    to_hourly_table,
    to_events_table,
)

# -------------------------------
# Loader: upsert with "replace recent days" semantics
# -------------------------------
from src.load.to_postgres import upsert_replace_recent_days

# Capture "today" once for the whole job
TODAY = date.today()

# -------------------------------
# Refresh windows (how far back to re-pull data)
# -------------------------------
DAYS_DAILY  = 7    # Rebuild last 7 days of daily ridership + weather
DAYS_HOURLY = 7    # Rebuild last 7 days of hourly data
DAYS_EVENTS = 14   # Rebuild last 14 days of events (can be delayed/late)


def daily_refresh():
    """
    Run a daily incremental refresh:

      - Pull last N days of data from source APIs
      - Clean them into canonical tables
      - Delete + upsert for those last N days (idempotent refresh)
    """
    # Compute start dates for each feed (as ISO strings)
    s_daily = (TODAY - timedelta(days=DAYS_DAILY)).isoformat()
    s_hour  = (TODAY - timedelta(days=DAYS_HOURLY)).isoformat()
    s_ev    = (TODAY - timedelta(days=DAYS_EVENTS)).isoformat()
    e       = TODAY.isoformat()  # common end date = today

    # ----------------------------
    # Daily ridership + weather
    # ----------------------------
    # Extract + transform for daily ridership
    mta = to_ridership_table(fetch_mta_daily(s_daily, e))
    # Extract + transform for daily weather
    wx  = to_weather_table(fetch_noaa_daily(s_daily, e))

    # Replace last `DAYS_DAILY` days of data in each table with fresh rows
    upsert_replace_recent_days(
        mta,
        "fact_ridership_daily",
        date_col="date",
        days=DAYS_DAILY,
        pkey=["date", "mode"],
    )
    upsert_replace_recent_days(
        wx,
        "dim_weather_daily",
        date_col="date",
        days=DAYS_DAILY,
        pkey="date",
    )
    print(f"[daily] refreshed last {DAYS_DAILY} days → r={len(mta)} w={len(wx)}")

    # ----------------------------
    # Hourly subway ridership (2025+)
    # ----------------------------
    hh = to_hourly_table(fetch_mta_hourly_by_borough(s_hour, e))
    upsert_replace_recent_days(
        hh,
        "fact_subway_hourly",
        date_col="date",
        days=DAYS_HOURLY,
        pkey=["date", "hour", "borough"],
    )
    print(f"[hourly] refreshed last {DAYS_HOURLY} days → h={len(hh)}")

    # ----------------------------
    # Events (can trickle in late → longer window)
    # ----------------------------
    ev = to_events_table(fetch_events_daily(s_ev, e))
    upsert_replace_recent_days(
        ev,
        "dim_events_daily",
        date_col="date",
        days=DAYS_EVENTS,
        pkey=["date", "borough"],
    )
    print(f"[events] refreshed last {DAYS_EVENTS} days → e={len(ev)}")


if __name__ == "__main__":
    # When run as a script: perform the daily refresh and print markers
    print("=== DAILY JOB START ===")
    daily_refresh()
    print("=== DAILY JOB COMPLETE ===")
