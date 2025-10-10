# src/extract/mta_hourly.py
from __future__ import annotations

import os
import time
from io import StringIO
from typing import Optional, Dict, Any, List

import pandas as pd
import requests

# Use BASE resource URLs (we'll append .json / .csv as needed)
DATASETS = [
    ("https://data.ny.gov/resource/wujg-7c2s", "2020-01-01", "2024-12-31"),  # 2020–2024
    ("https://data.ny.gov/resource/5wq4-mkjj", "2025-01-01", "2100-01-01"),  # 2025+
]

DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# Candidate column names that appear across versions
_TS_CANDS = ["transit_timestamp", "timestamp", "datetime", "date_time", "time", "dt"]
_BORO_CANDS = [
    "borough", "borough_name", "boroname", "complex_borough", "station_complex_borough",
    "station_borough", "boroughdesc", "borough_desc", "boro",
]
_RID_CANDS = [
    "ridership", "rides", "entries", "count", "total",
    "ridership_total", "total_ridership", "ridership_estimate", "ridership_count", "value",
]


def _pick(cands: List[str], cols: List[str]) -> str | None:
    for c in cands:
        if c in cols:
            return c
    return None


def _shape_and_aggregate(df: pd.DataFrame, url_base: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Normalize column names, filter to window, and aggregate to (date,hour,borough)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # Clean up column names
    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    ts_col = _pick(_TS_CANDS, cols)
    bor_col = _pick(_BORO_CANDS, cols)
    if not bor_col:
        # Last-ditch: any column that looks like borough
        for c in cols:
            lc = c.lower()
            if "boro" in lc or "borough" in lc:
                bor_col = c
                break
    rid_col = _pick(_RID_CANDS, cols)

    if not ts_col or not rid_col or not bor_col or bor_col not in df:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    # Types + local date filter
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col])
    mask = (df[ts_col] >= f"{start_date}T00:00:00") & (df[ts_col] <= f"{end_date}T23:59:59")
    df = df.loc[mask]
    if df.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    df["date"] = df[ts_col].dt.date
    df["hour"] = df[ts_col].dt.hour
    df["borough"] = df[bor_col].astype(str)
    df["riders"] = pd.to_numeric(df[rid_col], errors="coerce")

    out = (
        df.groupby(["date", "hour", "borough"], as_index=False)["riders"]
        .sum()
        .sort_values(["date", "hour", "borough"])
        .reset_index(drop=True)
    )
    out["source"] = "data.ny.gov/" + ("wujg-7c2s" if "wujg-7c2s" in url_base else "5wq4-mkjj")
    return out


def _fetch_raw_json(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """Page through the JSON endpoint."""
    limit, offset, page = 50000, 0, 0
    params = {
        "$order": "transit_timestamp ASC",
        "$select": "*",
        "$where": f"transit_timestamp >= '{start_date}T00:00:00' AND transit_timestamp <= '{end_date}T23:59:59'",
        "$limit": limit,
    }
    rows: List[Dict[str, Any]] = []

    while True:
        p = dict(params, **{"$offset": offset})
        r = requests.get(url_base + ".json", params=p, headers=headers, timeout=60)
        if r.status_code != 200:
            break
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
        page += 1
        if page > 4:  # safety for smoke runs
            break

    return pd.DataFrame.from_records(rows)


def _fetch_raw_csv(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """Same window, but pull as CSV (works reliably for wujg-7c2s)."""
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
        df = pd.read_csv(StringIO(r.text))
        if df.empty:
            break
        frames.append(df)
        if len(df) < limit:
            break
        offset += limit
        page += 1
        if page > 4:
            break

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_raw(url_base: str, start_date: str, end_date: str, headers: Dict[str, str]) -> pd.DataFrame:
    """Try JSON first; if empty, fall back to CSV. Then shape/aggregate."""
    debug = os.getenv("MTA_HOURLY_DEBUG") == "1"
    df = _fetch_raw_json(url_base, start_date, end_date, headers)
    if debug:
        print(f"DEBUG JSON {url_base}: {len(df)} rows")
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
    """Fetch hourly subway ridership aggregated to (date, hour, borough)."""
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    tok = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if tok:
        headers["X-App-Token"] = tok

    frames: List[pd.DataFrame] = []
    for url_base, ds_start, ds_end in DATASETS:
        s = max(start_date, ds_start)
        e = min(end_date, ds_end)
        if s > e:
            continue
        df = _fetch_raw(url_base, s, e, headers)
        if not df.empty:
            frames.append(df[["date", "hour", "borough", "riders", "source"]])

    if not frames:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["date", "hour", "borough"], as_index=False)["riders"]
        .sum()
        .sort_values(["date", "hour", "borough"])
        .reset_index(drop=True)
    )
    return out


if __name__ == "__main__":
    # quick local test
    print(fetch_mta_hourly_by_borough("2024-12-31", "2025-01-02").head(12))
