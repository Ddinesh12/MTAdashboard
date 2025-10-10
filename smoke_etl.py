# smoke_etl.py
from dotenv import load_dotenv
load_dotenv()

import os, sys
from pathlib import Path
from datetime import date

# --- import local src package ---
ROOT = Path(__file__).parent.resolve()
SRC  = ROOT / "src"
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
if str(SRC)  not in sys.path: sys.path.insert(0, str(SRC))

# --- extractors ---
from src.extract.mta_daily import fetch_mta_daily
from src.extract.noaa_daily import fetch_noaa_daily
from src.extract.mta_hourly import fetch_mta_hourly_by_borough
from src.extract.events_daily import fetch_events_daily

# --- transformers ---
from src.transform.clean import (
    to_ridership_table,
    to_weather_table,
    to_hourly_table,
    to_events_table,
)

# --- loader ---
from src.load.to_postgres import upsert

# ------------------------
# Windows
# ------------------------
START_D, END_D = "2024-09-01", "2025-10-01"   # daily & weather ~ last 13 months
START_H, END_H = "2025-08-01", "2025-10-01"   # hourly inside last 60 days window today
START_E, END_E = "2024-01-01", "2025-10-01"   # events ~ last 21 months


def main():
    # --- Extract ---
    mta_daily_raw  = fetch_mta_daily(START_D, END_D)
    noaa_daily_raw = fetch_noaa_daily(START_D, END_D)
    hourly_raw     = fetch_mta_hourly_by_borough(START_H, END_H)
    events_raw     = fetch_events_daily(START_E, END_E)

    # --- Transform ---
    mta_daily    = to_ridership_table(mta_daily_raw)
    weather      = to_weather_table(noaa_daily_raw)
    hourly       = to_hourly_table(hourly_raw)
    events_daily = to_events_table(events_raw)

    # --- Load (UPSERT) ---
    upsert(mta_daily,    "fact_ridership_daily", pkey=["date", "mode"])
    upsert(weather,      "dim_weather_daily",    pkey="date")
    upsert(hourly,       "fact_subway_hourly",   pkey=["date", "hour", "borough"])
    upsert(events_daily, "dim_events_daily",     pkey=["date", "borough"])

    # --- Report ---
    print(
        "Smoke ETL finished:",
        f"{len(mta_daily)} daily ridership rows,",
        f"{len(weather)} weather rows,",
        f"{len(hourly)} hourly rows,",
        f"{len(events_daily)} events rows."
    )

    if len(hourly) == 0:
        print("NOTE: hourly returned 0 rows — ensure your window overlaps 2025 and SOCRATA_APP_TOKEN is set.")
    if len(events_daily) == 0:
        print("NOTE: events returned 0 rows — try EVENTS_DEBUG=1 and/or widen START_E..END_E.")

if __name__ == "__main__":
    main()
