# src/extract/mta_hourly.py
from __future__ import annotations

import os
import time
from io import StringIO
from typing import Optional, Dict, Any, List

import pandas as pd
import requests

# Two separate hourly ridership datasets, with different ID and date coverage:
# - wujg-7c2s: 2020–2024
# - 5wq4-mkjj: 2025+ (forward-looking)
# We treat them as one logical source and stitch them together by date.
DATASETS = [
    ("https://data.ny.gov/resource/wujg-7c2s", "2020-01-01", "2024-12-31"),  # 2020–2024
    ("https://data.ny.gov/resource/5wq4-mkjj", "2025-01-01", "2100-01-01"),  # 2025+
]

# User-Agent string to identify this client to the API
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# Candidate column names for the timestamp field across different schema versions
_TS_CANDS = ["transit_timestamp", "timestamp", "datetime", "date_time", "time", "dt"]

# Candidate column names for the borough field
_BORO_CANDS = [
    "borough", "borough_name", "boroname", "complex_borough", "station_complex_borough",
    "station_borough", "boroughdesc", "borough_desc", "boro",
]

# Candidate column names for the ridership metric
_RID_CANDS = [
    "ridership", "rides", "entries", "count", "total",
    "ridership_total", "total_ridership", "ridership_estimate", "ridership_count", "value",
]


def _pick(cands: List[str], cols: List[str]) -> str | None:
    """
    Return the first candidate name that exists in `cols`, or None.

    This is a small helper to handle schema drift, where the dataset may rename
    a column but still represent the same concept (timestamp, borough, ridership).
    """
    for c in cands:
        if c in cols:
            return c
    return None


def _shape_and_aggregate(df: pd.DataFrame, url_base: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Normalize column names, filter to [start_date, end_date],
    and aggregate to (date, hour, borough).

    Output columns:
      - date (Python date)
      - hour (0–23)
      - borough (string as provided; cleaned elsewhere if needed)
      - riders (sum of ridership in that (date, hour, borough))
      - source (which dataset ID this came from)
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # Clean up column names: ensure all are strings and trim whitespace
    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    # Detect timestamp, borough, and ridership columns
    ts_col = _pick(_TS_CANDS, cols)
    bor_col = _pick(_BORO_CANDS, cols)
    if not bor_col:
        # Last-ditch attempt: any column whose name looks like borough
        for c in cols:
            lc = c.lower()
            if "boro" in lc or "borough" in lc:
                bor_col = c
                break
    rid_col = _pick(_RID_CANDS, cols)

    # If we can't identify required columns, return an empty result
    if not ts_col or not rid_col or not bor_col or bor_col not in df:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # -------------------------------------------------------------------------
    # Type conversion + local date filtering
    # -------------------------------------------------------------------------

    # Parse timestamps; invalid values become NaT and are dropped
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col])

    # Keep only rows whose timestamp is within [start_date, end_date]
    mask = (df[ts_col] >= f"{start_date}T00:00:00") & (df[ts_col] <= f"{end_date}T23:59:59")
    df = df.loc[mask]
    if df.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # Derive separate date and hour fields for grouping
    df["date"] = df[ts_col].dt.date
    df["hour"] = df[ts_col].dt.hour
    # Preserve borough as string
    df["borough"] = df[bor_col].astype(str)
    # Ensure ridership is numeric
    df["riders"] = pd.to_numeric(df[rid_col], errors="coerce")

    # Group to (date, hour, borough) and sum riders
    out = (
        df.groupby(["date", "hour", "borough"], as_index=False)["riders"]
        .sum()
        .sort_values(["date", "hour", "borough"])
        .reset_index(drop=True)
    )

    # Attach a source tag so we know which Socrata dataset was used
    out["source"] = "data.ny.gov/" + ("wujg-7c2s" if "wujg-7c2s" in url_base else "5wq4-mkjj")
    return out


def _fetch_raw_json(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """
    Fetch hourly ridership as JSON from the Socrata endpoint, paging through results.

    Uses a $where filter on transit_timestamp for the given [start_date, end_date]
    and pages with $limit / $offset up to a safety limit.
    """
    limit, offset, page = 50000, 0, 0
    params = {
        "$order": "transit_timestamp ASC",
        "$select": "*",
        "$where": f"transit_timestamp >= '{start_date}T00:00:00' AND transit_timestamp <= '{end_date}T23:59:59'",
        "$limit": limit,
    }
    rows: List[Dict[str, Any]] = []

    while True:
        # Add current offset for paging
        p = dict(params, **{"$offset": offset})
        r = requests.get(url_base + ".json", params=p, headers=headers, timeout=60)
        if r.status_code != 200:
            break

        chunk = r.json()
        if not chunk:
            break

        rows.extend(chunk)

        # If we receive fewer than limit rows, we've hit the end
        if len(chunk) < limit:
            break

        offset += limit
        page += 1
        # Hard safety stop for smoke runs; prevents unbounded scans
        if page > 4:
            break

    return pd.DataFrame.from_records(rows)


def _fetch_raw_csv(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """
    Fetch hourly ridership as CSV from the Socrata endpoint, with the same date window.

    This is a fallback path (particularly for wujg-7c2s), using a minimal select:
    transit_timestamp, borough, ridership.
    """
    limit, offset, page = 50000, 0, 0
    frames: List[pd.DataFrame] = []

    base_params = {
        "$order": "transit_timestamp ASC",
        "$select": "transit_timestamp, borough, ridership",
        "$where": f"transit_timestamp >= '{start_date}T00:00:00' AND transit_timestamp <= '{end_date}T23:59:59'",
        "$limit": limit,
    }

    while True:
        p = dict(base_params, **{"$offset": offset})
        r = requests.get(url_base + ".csv", params=p, headers=headers, timeout=60)
        if r.status_code != 200:
            break

        # Read CSV response into a DataFrame from the response text
        df = pd.read_csv(StringIO(r.text))
        if df.empty:
            break

        frames.append(df)

        # Stop if we got fewer than limit rows
        if len(df) < limit:
            break

        offset += limit
        page += 1
        if page > 4:
            break

    # Combine all pages into a single DataFrame (or empty if none)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_raw(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """
    Fetch raw hourly data from a given dataset base URL, then shape it.

    Steps:
      1. Try JSON endpoint.
      2. If JSON returns nothing, try the CSV endpoint as a fallback.
      3. Pass the resulting DataFrame through _shape_and_aggregate to get
         (date, hour, borough, riders, source).
    """
    debug = os.getenv("MTA_HOURLY_DEBUG") == "1"

    # First attempt: JSON
    df = _fetch_raw_json(url_base, start_date, end_date, headers)
    if debug:
        print(f"DEBUG JSON {url_base}: {len(df)} rows")

    # If JSON returned nothing, try CSV
    if df.empty:
        df = _fetch_raw_csv(url_base, start_date, end_date, headers)
        if debug:
            print(f"DEBUG CSV  {url_base}: {len(df)} rows")

    return _shape_and_aggregate(df, url_base, start_date, end_date)


def fetch_mta_hourly_by_borough(
    start_date: str,
    end_date: str,
    app_token: Optional[str] = None,
) -> pd.DataFrame:
    """
    Public function to fetch hourly subway ridership, aggregated to (date, hour, borough).

    - Handles stitching together multiple datasets (2020–2024 and 2025+).
    - For each dataset, restricts the requested window to its valid date range.
    - Calls _fetch_raw() to retrieve and aggregate data.
    - Finally, concatenates everything and re-aggregates to ensure a clean result.
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    tok = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if tok:
        headers["X-App-Token"] = tok

    frames: List[pd.DataFrame] = []

    # Loop over each dataset and pull the overlapping slice for [start_date, end_date]
    for url_base, ds_start, ds_end in DATASETS:
        # Compute overlap between requested window and dataset's coverage
        s = max(start_date, ds_start)
        e = min(end_date, ds_end)
        if s > e:
            # No overlap; skip this dataset
            continue

        df = _fetch_raw(url_base, s, e, headers)
        if not df.empty:
            frames.append(df[["date", "hour", "borough", "riders", "source"]])

    # If nothing was fetched from any dataset, return an empty frame with the expected schema
    if not frames:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # Combine all dataset slices into one DataFrame
    out = pd.concat(frames, ignore_index=True)

    # Re-aggregate to guard against overlaps or duplicates between datasets
    out = (
        out.groupby(["date", "hour", "borough"], as_index=False)["riders"]
        .sum()
        .sort_values(["date", "hour", "borough"])
        .reset_index(drop=True)
    )
    return out


if __name__ == "__main__":
    # Quick local test: fetch a small window around the dataset boundary
    print(fetch_mta_hourly_by_borough("2024-12-31", "2025-01-02").head(12))
