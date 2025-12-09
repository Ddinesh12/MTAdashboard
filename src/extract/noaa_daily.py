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

# User-Agent string so NOAA can identify this client
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# Central Park weather station ID (first-order station for Manhattan)
# USW00094728 is widely used as the NYC Central Park station
DEFAULT_STATION = "USW00094728"


def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Make a GET request to the NOAA API with basic retry logic.

    - Try up to `max_retries` times.
    - On HTTP 200, return the parsed JSON.
    - On transient errors (429 / 5xx), back off and retry.
    - On other errors, raise immediately.
    """
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=45)
        if r.status_code == 200:
            return r.json()
        # Transient error: rate limit or server-side issue – wait and try again
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt)
            continue
        # Non-transient error: let it raise with details
        r.raise_for_status()

    # Final attempt after exhausting retries
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
    Fetch NOAA/NCEI GHCNd daily summaries for a weather station.

    By default, this uses the Central Park station (USW00094728)
    and aligns the start date with the MTA series.

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' inclusive, defaults to 2020-03-01
    end_date   : 'YYYY-MM-DD' inclusive, defaults to today
    station    : GHCNd station id (e.g., 'USW00094728' Central Park)
    units      : 'standard' (F/inches) or 'metric'
    token      : Optional NCEI token (not strictly required for this endpoint)

    Returns
    -------
    DataFrame with columns:
      ['date', 'station_id', 'tmax_f', 'tmin_f', 'prcp_in', 'snow_in']
    """
    # Default window: start of COVID period → today
    if start_date is None:
        start_date = "2020-03-01"
    if end_date is None:
        end_date = date.today().isoformat()

    # Basic headers with User-Agent
    headers = {"User-Agent": DEFAULT_USER_AGENT}

    # If we have a NOAA token (env or param), include it in headers
    token = token or os.getenv("NOAA_TOKEN")
    if token:
        headers["token"] = token

    # Build query parameters according to NOAA's Access Data Service API
    params = {
        "dataset": "daily-summaries",          # GHCNd daily summaries
        "stations": station,                   # station ID (Central Park by default)
        "startDate": start_date,
        "endDate": end_date,
        "dataTypes": "TMAX,TMIN,PRCP,SNOW",    # we only request max/min temp, precip, snow
        "units": units,                        # 'standard' => Fahrenheit / inches
        "format": "json",                      # JSON output for easy parsing
    }

    # Call NOAA with retry-safe HTTP helper
    records = _http_get(BASE_URL, params, headers)
    if not records:
        # If nothing comes back, return an empty frame with expected columns
        return pd.DataFrame(columns=["date", "station_id", "tmax_f", "tmin_f", "prcp_in", "snow_in"])

    # Turn JSON records into a DataFrame
    df = pd.DataFrame.from_records(records)

    # Normalize column names coming from NOAA into our internal naming scheme
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

    # Convert date to Python date objects
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Convert numeric weather fields (TMAX, TMIN, PRCP, SNOW) to numeric types
    for c in ["tmax_f", "tmin_f", "prcp_in", "snow_in"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ensure all expected columns exist (fill missing ones with NA)
    keep = ["date", "station_id", "tmax_f", "tmin_f", "prcp_in", "snow_in"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    # Return only the columns we care about, sorted by date
    return df[keep].sort_values("date").reset_index(drop=True)


if __name__ == "__main__":
    # Simple local smoke test: fetch a small window and print the head
    sample = fetch_noaa_daily(start_date="2024-01-01", end_date="2024-01-07")
    print(sample.head())
