# extract/noaa_daily.py
from __future__ import annotations

from datetime import date
from typing import Optional, Dict, Any, List
import time
import os

import requests
import pandas as pd

# NCEI Access Data Service (dataset=daily-summaries)
# Docs: https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation
BASE_URL = "https://www.ncei.noaa.gov/access/services/data/v1"
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# Central Park weather station ID (first-order station for Manhattan)
# USW00094728 is widely used for NYC Central Park
DEFAULT_STATION = "USW00094728"

def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3) -> List[Dict[str, Any]]:
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=45)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt)
            continue
        r.raise_for_status()
    r = requests.get(url, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    return r.json()

def fetch_noaa_daily(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    station: str = DEFAULT_STATION,
    units: str = "standard",  # Fahrenheit/inches
    token: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch NOAA/NCEI GHCNd daily summaries for a station (Central Park by default).

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' inclusive, defaults to 2020-03-01 to align with MTA series
    end_date   : 'YYYY-MM-DD' inclusive, defaults to today
    station    : GHCNd station id (e.g., 'USW00094728' Central Park)
    units      : 'standard' (F/inches) or 'metric'
    token      : Optional NCEI token (not required for this endpoint)

    Returns
    -------
    DataFrame with columns: ['date','station_id','tmax_f','tmin_f','prcp_in','snow_in']
    """
    if start_date is None:
        start_date = "2020-03-01"
    if end_date is None:
        end_date = date.today().isoformat()

    headers = {"User-Agent": DEFAULT_USER_AGENT}
    # If you have a token you can pass it, some client environments prefer it in header 'token'
    token = token or os.getenv("NOAA_TOKEN")
    if token:
        headers["token"] = token

    params = {
        "dataset": "daily-summaries",
        "stations": station,
        "startDate": start_date,
        "endDate": end_date,
        "dataTypes": "TMAX,TMIN,PRCP,SNOW",
        "units": units,
        "format": "json",
    }

    records = _http_get(BASE_URL, params, headers)
    if not records:
        return pd.DataFrame(columns=["date","station_id","tmax_f","tmin_f","prcp_in","snow_in"])

    df = pd.DataFrame.from_records(records)
    # Normalize column names
    df.rename(
        columns={
            "DATE": "date",
            "STATION": "station_id",
            "TMAX": "tmax_f",
            "TMIN": "tmin_f",
            "PRCP": "prcp_in",
            "SNOW": "snow_in",
        },
        inplace=True,
    )

    # Types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ["tmax_f", "tmin_f", "prcp_in", "snow_in"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep only relevant columns
    keep = ["date", "station_id", "tmax_f", "tmin_f", "prcp_in", "snow_in"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    return df[keep].sort_values("date").reset_index(drop=True)

if __name__ == "__main__":
    sample = fetch_noaa_daily(start_date="2024-01-01", end_date="2024-01-07")
    print(sample.head())
