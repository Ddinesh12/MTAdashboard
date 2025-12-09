# src/extract/mta_daily.py
from __future__ import annotations

import os, time
from datetime import date
from typing import Optional, Dict, Any, List
import requests, pandas as pd

# Socrata endpoint for daily MTA ridership (subway + bus)
BASE_URL = "https://data.ny.gov/resource/sayj-mze2.json"

# User-Agent string so the API can identify this client
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# Candidate column names for subway ridership in "wide" schemas
# (dataset has changed names over time)
SUBWAY_CANDS = [
    "subways_total_estimated_ridership",
    "subway_total_estimated_ridership",
    "subways", "subway_ridership",
]

# Candidate column names for bus ridership in "wide" schemas
BUS_CANDS = [
    "buses_total_estimated_ridership",
    "buses_total_ridership",
    "buses", "bus_ridership",
]

# Candidate column names for the date field
DATE_CANDS = ["date", "as_of", "report_date"]

# Candidate column names for the numeric ridership field in "long" schemas
LONG_VALUE_CANDS = ["riders", "ridership", "count", "value", "total"]


def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3):
    """
    Make a GET request with basic retry logic.

    - Try up to `max_retries` times.
    - If status is 200, return parsed JSON immediately.
    - If we see transient errors (429, 5xx), back off and retry.
    - For other errors, raise an HTTPError with details.
    """
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=45)
        if r.status_code == 200:
            return r.json()
        # Handle rate limits/server errors with exponential-ish backoff
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt)
            continue
        # Non-transient error: raise immediately with message
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}")

    # Final attempt after all retries; let this raise if it fails
    r = requests.get(url, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    return r.json()


def _pick(candidates: List[str], columns: List[str]) -> Optional[str]:
    """
    Helper to pick the first candidate name that actually exists in `columns`.

    Used to handle schema drift: the dataset may rename fields, so we search
    through a list of possible names and pick the first match.
    """
    for c in candidates:
        if c in columns:
            return c
    return None


def _normalize_mode(s: pd.Series) -> pd.Series:
    """
    Normalize the 'mode' field to a clean set: {"subway", "bus"}.

    - Convert to lowercase strings and strip whitespace.
    - Map variants like "subways" → "subway" and "buses" → "bus".
    - Anything not in {"subway","bus"} is set to NaN and dropped later.
    """
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
    Fetch daily systemwide subway & bus ridership totals from the MTA dataset.

    The dataset has appeared in two main shapes:
      - "long": one row per (date, mode) with a ridership value column.
      - "wide": one row per date with separate subway and bus columns.

    This function:
      1. Pulls all rows 
      2. Filters them to the requested date range.
      3. Detects whether the data is long or wide.
      4. Normalizes into a common long format with columns:
         ['date', 'mode', 'riders', 'source'].
    """
    # Default date range: from early pandemic forward to today
    if start_date is None:
        start_date = "2020-03-01"
    if end_date is None:
        end_date = date.today().isoformat()

    # Build HTTP headers
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    token = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    # Keep query simple to avoid SoQL errors if field IDs or schema change
    records = _http_get(
        BASE_URL,
        params={"$order": "date ASC", "$limit": 50000},
        headers=headers,
    )
    if not records:
        # Return empty DataFrame with the expected schema if nothing comes back
        return pd.DataFrame(columns=["date", "mode", "riders", "source"])

    # Convert JSON records to DataFrame
    df = pd.DataFrame.from_records(records)
    cols = list(df.columns)

    # Detect which column represents the date, fall back to "date" if needed
    date_col = _pick(DATE_CANDS, cols) or "date"
    # Parse to datetime.date; invalid values become NaT and then NaN
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # Filter to the requested [start_date, end_date] window in pandas
    sdate, edate = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
    df = df[(df[date_col] >= sdate) & (df[date_col] <= edate)]

    # -------------------------------------------------------------------------
    # CASE A: Dataset is already "long" => columns (date, mode, value)
    # -------------------------------------------------------------------------
    if "mode" in cols:
        # Find which column holds the numeric ridership values
        val_col = _pick(LONG_VALUE_CANDS, cols)
        if val_col is None:
            raise RuntimeError(
                f"Found 'mode' but no value column among {LONG_VALUE_CANDS}. Columns: {cols}"
            )

        # Keep only the date, mode and value columns
        out = df[[date_col, "mode", val_col]].copy()
        # Normalize mode labels (subway / bus)
        out["mode"] = _normalize_mode(out["mode"])
        # Convert ridership to numeric (coerce invalid to NaN)
        out[val_col] = pd.to_numeric(out[val_col], errors="coerce")

        # Drop rows with missing date or mode
        out = out.dropna(subset=[date_col, "mode"])
        # Rename columns to standard names
        out = out.rename(columns={date_col: "date", val_col: "riders"})
        # Attach a source column for traceability
        out["source"] = "data.ny.gov/sayj-mze2"

        # Return tidy DataFrame sorted by date/mode
        return (
            out[["date", "mode", "riders", "source"]]
            .sort_values(["date", "mode"])
            .reset_index(drop=True)
        )

    # -------------------------------------------------------------------------
    # CASE B: Dataset is "wide" => separate subway + bus columns per date
    # -------------------------------------------------------------------------
    sub_col = _pick(SUBWAY_CANDS, cols)
    bus_col = _pick(BUS_CANDS, cols)

    if sub_col and bus_col:
        # Ensure subway and bus columns are numeric
        df[sub_col] = pd.to_numeric(df[sub_col], errors="coerce")
        df[bus_col] = pd.to_numeric(df[bus_col], errors="coerce")

        # Melt from wide to long: (date, subway, bus) → multiple rows with "mode_raw"
        tidy = pd.melt(
            df[[date_col, sub_col, bus_col]],
            id_vars=[date_col],
            value_vars=[sub_col, bus_col],
            var_name="mode_raw",
            value_name="riders",
        )

        # Map the original column names to clean mode labels
        map_modes = {sub_col: "subway", bus_col: "bus"}
        tidy["mode"] = tidy["mode_raw"].map(map_modes)

        # Drop helper column, standardize column names
        tidy = tidy.drop(columns=["mode_raw"]).rename(columns={date_col: "date"})
        tidy["source"] = "data.ny.gov/sayj-mze2"

        # Return tidy long-format DataFrame
        return (
            tidy[["date", "mode", "riders", "source"]]
            .sort_values(["date", "mode"])
            .reset_index(drop=True)
        )

    # -------------------------------------------------------------------------
    # If we reach here, the dataset shape is something unexpected.
    # -------------------------------------------------------------------------
    raise RuntimeError(f"Unexpected schema for sayj-mze2. Columns present: {cols}")
