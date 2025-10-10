# src/extract/mta_daily.py
from __future__ import annotations

import os, time
from datetime import date
from typing import Optional, Dict, Any, List
import requests, pandas as pd

BASE_URL = "https://data.ny.gov/resource/sayj-mze2.json"
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# older/wide column candidates (dataset used to change names)
SUBWAY_CANDS = [
    "subways_total_estimated_ridership",
    "subway_total_estimated_ridership",
    "subways", "subway_ridership",
]
BUS_CANDS = [
    "buses_total_estimated_ridership",
    "buses_total_ridership",
    "buses", "bus_ridership",
]
DATE_CANDS = ["date", "as_of", "report_date"]
LONG_VALUE_CANDS = ["riders", "ridership", "count", "value", "total"]

def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=45)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt); continue
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}")
    r = requests.get(url, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    return r.json()

def _pick(candidates: List[str], columns: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None

def _normalize_mode(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip().str.lower()
    out = out.replace({
        "subways": "subway", "subway": "subway",
        "buses": "bus", "bus": "bus"
    })
    return out.where(out.isin(["subway", "bus"]))

def fetch_mta_daily(start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    app_token: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch daily systemwide subway & bus ridership totals.
    Handles both wide (subway+bus columns) and long (date/mode/value) schemas.
    Returns columns: ['date','mode','riders','source']
    """
    if start_date is None: start_date = "2020-03-01"
    if end_date   is None: end_date   = date.today().isoformat()

    headers = {"User-Agent": DEFAULT_USER_AGENT}
    token = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    # keep query simple to avoid SoQL 400s from field-id shifts
    records = _http_get(
        BASE_URL,
        params={"$order": "date ASC", "$limit": 50000},
        headers=headers,
    )
    if not records:
        return pd.DataFrame(columns=["date","mode","riders","source"])

    df = pd.DataFrame.from_records(records)
    cols = list(df.columns)
    date_col = _pick(DATE_CANDS, cols) or "date"
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # filter date window in pandas
    sdate, edate = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    df = df[(df[date_col] >= sdate) & (df[date_col] <= edate)]

    # --- CASE A: already long (date, mode, value) ---
    if "mode" in cols:
        val_col = _pick(LONG_VALUE_CANDS, cols)
        if val_col is None:
            raise RuntimeError(f"Found 'mode' but no value column among {LONG_VALUE_CANDS}. Columns: {cols}")

        out = df[[date_col, "mode", val_col]].copy()
        out["mode"] = _normalize_mode(out["mode"])
        out[val_col] = pd.to_numeric(out[val_col], errors="coerce")
        out = out.dropna(subset=[date_col, "mode"])
        out = out.rename(columns={date_col: "date", val_col: "riders"})
        out["source"] = "data.ny.gov/sayj-mze2"
        return out[["date","mode","riders","source"]].sort_values(["date","mode"]).reset_index(drop=True)

    # --- CASE B: wide columns (subway + bus) ---
    sub_col = _pick(SUBWAY_CANDS, cols)
    bus_col = _pick(BUS_CANDS, cols)
    if sub_col and bus_col:
        df[sub_col] = pd.to_numeric(df[sub_col], errors="coerce")
        df[bus_col] = pd.to_numeric(df[bus_col], errors="coerce")
        tidy = pd.melt(
            df[[date_col, sub_col, bus_col]],
            id_vars=[date_col],
            value_vars=[sub_col, bus_col],
            var_name="mode_raw",
            value_name="riders",
        )
        map_modes = {sub_col: "subway", bus_col: "bus"}
        tidy["mode"] = tidy["mode_raw"].map(map_modes)
        tidy = tidy.drop(columns=["mode_raw"]).rename(columns={date_col: "date"})
        tidy["source"] = "data.ny.gov/sayj-mze2"
        return tidy[["date","mode","riders","source"]].sort_values(["date","mode"]).reset_index(drop=True)

    # --- otherwise, we don't know this schema ---
    raise RuntimeError(f"Unexpected schema for sayj-mze2. Columns present: {cols}")
