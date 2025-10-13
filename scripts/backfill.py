# scripts/backfill.py
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
import os, sys

from dotenv import load_dotenv
load_dotenv()

# make local src importable
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

from src.load.to_postgres import upsert

# ------------------------
# Config: backfill windows
# ------------------------
TODAY = date.today()

# Daily: last 400 days
DAILY_START = (TODAY - timedelta(days=400)).isoformat()
DAILY_END   = TODAY.isoformat()

# Hourly: last 90 days (2025+)
HOURLY_START = (TODAY - timedelta(days=90)).isoformat()
HOURLY_END   = TODAY.isoformat()

# Events: last 365 days
EVENTS_START = (TODAY - timedelta(days=365)).isoformat()
EVENTS_END   = TODAY.isoformat()

# Chunk sizes (keep API + memory friendly)
CHUNK_DAILY_DAYS  = 60   # ridership/weather
CHUNK_HOURLY_DAYS = 7    # hourly is big
CHUNK_EVENTS_DAYS = 30

def daterange_chunks(start_str: str, end_str: str, chunk_days: int):
    s = date.fromisoformat(start_str)
    e = date.fromisoformat(end_str)
    d = s
    while d <= e:
        chunk_end = min(d + timedelta(days=chunk_days-1), e)
        yield d.isoformat(), chunk_end.isoformat()
        d = chunk_end + timedelta(days=1)

def backfill_daily():
    total_r = total_w = 0
    for s,e in daterange_chunks(DAILY_START, DAILY_END, CHUNK_DAILY_DAYS):
        mta_raw  = fetch_mta_daily(s, e)
        wx_raw   = fetch_noaa_daily(s, e)
        mta      = to_ridership_table(mta_raw)
        wx       = to_weather_table(wx_raw)
        total_r += upsert(mta, "fact_ridership_daily", pkey=["date","mode"])
        total_w += upsert(wx,  "dim_weather_daily",    pkey="date")
        print(f"[daily] {s}..{e}  ridership={len(mta)}  weather={len(wx)}")
    print(f"[daily] DONE  staged rows → ridership={total_r}, weather={total_w}")

def backfill_hourly():
    total_h = 0
    for s,e in daterange_chunks(HOURLY_START, HOURLY_END, CHUNK_HOURLY_DAYS):
        hh_raw = fetch_mta_hourly_by_borough(s, e)
        hh     = to_hourly_table(hh_raw)
        total_h += upsert(hh, "fact_subway_hourly", pkey=["date","hour","borough"])
        print(f"[hourly] {s}..{e}  hourly_rows={len(hh)}")
    print(f"[hourly] DONE  staged rows → hourly={total_h}")

def backfill_events():
    total_e = 0
    for s,e in daterange_chunks(EVENTS_START, EVENTS_END, CHUNK_EVENTS_DAYS):
        ev_raw = fetch_events_daily(s, e)
        ev     = to_events_table(ev_raw)
        total_e += upsert(ev, "dim_events_daily", pkey=["date","borough"])
        print(f"[events] {s}..{e}  event_rows={len(ev)}")
    print(f"[events] DONE  staged rows → events={total_e}")

if __name__ == "__main__":
    print("=== BACKFILL START ===")
    backfill_daily()
    backfill_hourly()
    backfill_events()
    print("=== BACKFILL COMPLETE ===")
