# src/extract/events_daily.py
from __future__ import annotations
import os, time
from typing import Dict, Any, List, Optional
import pandas as pd
import requests

# -----------------------------------------------------------------------------
# CONFIG / CONSTANTS
# -----------------------------------------------------------------------------

# NYC Permitted Event Information (Socrata) – this dataset lists permitted events
NYC_EVENTS_URL = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"

# Identify this script to the API – polite + helps with monitoring on their side
DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"

# We want to normalize boroughs into this closed set of clean labels
VALID_BOROUGHS = {"Bronx","Brooklyn","Manhattan","Queens","Staten Island"}

# Map all the messy borough encodings in the raw data into our clean labels
# e.g. "MN", "MANHATTAN" → "Manhattan"; "BK", "BROOKLYN" → "Brooklyn", etc.
BORO_MAP = {
    "MN": "Manhattan", "MANHATTAN": "Manhattan",
    "BX": "Bronx",     "BRONX": "Bronx",
    "BK": "Brooklyn",  "BKLN": "Brooklyn", "BROOKLYN": "Brooklyn",
    "QN": "Queens",    "QUEENS": "Queens",
    "SI": "Staten Island", "S.I.": "Staten Island",
    "STATEN ISLAND": "Staten Island", "STATENISLAND": "Staten Island",
}

# List of possible "date-like" columns in this dataset.
# The schema can change, so we pick the FIRST one we actually see in the API.
PREFERRED_DATE_COLS = [
    "startdatetime",       # often present as a full timestamp
    "start_date",          # sometimes a (date or timestamp) field
    "event_date",          # date-only in some rows
    "begin_date",          # alt naming sometimes seen
    "starttime",           # time-only paired with date (rare)
    "date",                # generic
]

# Candidate names for the borough column – again, schema can drift
BORO_CANDS = [
    "borough", "event_borough", "borough_name", "boroname",
    "borocode", "borough_desc", "boroughdescription"
]

# -----------------------------------------------------------------------------
# LOW-LEVEL HELPERS
# -----------------------------------------------------------------------------

def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3):
    """
    Robust HTTP GET wrapper with basic retry logic.

    - Tries the request up to `max_retries` times for transient errors:
      (429, 500, 502, 503, 504).
    - If it gets a 200 OK, returns the parsed JSON immediately.
    - If it gets a non-transient error (e.g. 400), it raises an exception.
    - After exhausting retries, it makes one final attempt and raises if that fails.
    """
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code == 200:
            return r.json()
        # Transient errors – back off and retry
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt)
            continue
        # Any other error is treated as fatal
        r.raise_for_status()

    # Final attempt after all retries – if this fails, we let it raise
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


def _pick(cands: List[str], cols: List[str]) -> Optional[str]:
    """
    Given a list of candidate names and the actual columns from the API,
    return the first candidate that exists.

    If none match exactly, fall back to a very loose substring match.

    This is our "schema detection" helper – it lets us adapt if NYC renames
    a column from e.g. 'event_date' to 'event_date_time'.
    """
    # First, look for an exact match
    for c in cands:
        if c in cols:
            return c
    # If no exact match, try fuzzy substring matching (very loose)
    for c in cols:
        lc = c.lower()
        for k in cands:
            if k in lc:
                return c
    return None


def _normalize_borough(series: pd.Series) -> pd.Series:
    """
    Normalize raw borough values into our clean set (VALID_BOROUGHS).

    Steps:
    - Convert everything to string, strip whitespace.
    - Use BORO_MAP to map codes/variants (MN, BROOKLYN, etc.) into a standard name.
    - Anything not in VALID_BOROUGHS is set to NaN and dropped later.

    """
    b = series.astype(str).str.strip()
    b = b.map(lambda s: BORO_MAP.get(s.upper(), s.title()))
    return b.where(b.isin(VALID_BOROUGHS))


def _detect_columns(url: str, headers: Dict[str, str]) -> List[str]:
    """
    Hit the API with $limit=1 to discover the *actual* column names currently in use.

    Socrata schemas can evolve, so instead of trusting docs, we introspect one row
    and look at its keys.
    """
    # fetch 1 row to see actual field names
    probe = _http_get(url, {"$limit": 1}, headers)
    if isinstance(probe, list) and probe:
        return list(probe[0].keys())
    return []


def _safe_between(field: str, start_date: str, end_date: str) -> str:
    """
    Build a Socrata $where BETWEEN clause that matches the column's type.

    - If the field name looks like a timestamp (contains 'time' or 'datetime'),
      we use full datetime literals 'YYYY-MM-DDTHH:MM:SS'.
    - Otherwise, we use date-only literals 'YYYY-MM-DD'.

    This avoids type mismatch issues in the server-side filter.
    """
    # Use datetime literal if likely a timestamp field, else date literal.
    fld = field.lower()
    if "time" in fld or "datetime" in fld:
        return f"{field} between '{start_date}T00:00:00' and '{end_date}T23:59:59'"
    return f"{field} between '{start_date}' and '{end_date}'"


# -----------------------------------------------------------------------------
# CORE FETCH / TRANSFORM LOGIC
# -----------------------------------------------------------------------------

def _fetch_events(url: str, start_date: str, end_date: str, headers: Dict[str,str]) -> pd.DataFrame:
    """
    Core worker that:
      1. Detects which columns to treat as the event datetime and borough.
      2. Pages through the NYC events API for the requested date window.
      3. Parses datetimes and filters rows locally to [start_date, end_date].
      4. Normalizes borough names.
      5. Aggregates to (date, borough, event_count).

    Returns a tidy DataFrame with columns: date, borough, event_count.
    """
    debug = os.getenv("EVENTS_DEBUG") == "1"

    # 1) Detect real columns from the live API, then pick ONE date column that exists.
    cols = _detect_columns(url, headers)
    if debug:
        print("EVENTS DEBUG — detected columns (first row):", cols[:40])

    date_col = _pick(PREFERRED_DATE_COLS, cols)
    boro_col = _pick(BORO_CANDS, cols)

    # As a very last resort, pick any column whose name contains 'date'
    if not date_col:
        for c in cols:
            if "date" in c.lower():
                date_col = c
                break

    # 2) Page through the dataset using ONLY that date column in WHERE/ORDER.
    #    We prefer to filter server-side by date (for performance),
    #    but if the WHERE fails, we fall back to pulling broadly and filtering in pandas.
    rows: List[Dict[str, Any]] = []
    limit, offset, page = 50000, 0, 0
    params_base = {"$select": "*", "$limit": limit}
    where_ok = bool(date_col)   # whether we're currently attempting server-side WHERE

    while True:
        params = dict(params_base)
        params["$offset"] = offset
        if where_ok:
            params["$where"] = _safe_between(date_col, start_date, end_date)
            params["$order"] = date_col

        try:
            chunk = _http_get(url, params, headers)
        except requests.HTTPError:
            # If WHERE fails for any reason (e.g., type mismatch), disable it
            # and try again without WHERE, then we'll filter locally instead.
            if where_ok:
                where_ok = False
                continue
            else:
                # If we're already not using WHERE, re-raise the error.
                raise

        if not chunk:
            # No more rows returned – we're done
            break

        rows.extend(chunk)

        # If we got fewer than `limit` rows, we've reached the end
        if len(chunk) < limit:
            break

        offset += limit
        page += 1

        # Safety guard: don't accidentally scan millions of rows in a "smoke" run
        if page > 4:  # smoke safety
            break

    if debug:
        print(f"EVENTS DEBUG — pulled raw rows: {len(rows)} (where_ok={where_ok}, date_col={date_col})")

    # If we got nothing back, return an empty DataFrame with the expected schema
    if not rows:
        return pd.DataFrame(columns=["date","borough","event_count"])

    df = pd.DataFrame.from_records(rows)

    # 3) Build a usable datetime column (`_dt`) for local filtering.
    dt = None

    # First, try parsing whatever date_col we picked.
    if date_col and date_col in df.columns:
        dt = pd.to_datetime(df[date_col], errors="coerce")

    # If that failed, try some hard-coded alternates that tend to appear.
    if dt is None or dt.isna().all():
        for alt in ["start_date", "startdatetime", "event_date", "date"]:
            if alt in df.columns:
                dt = pd.to_datetime(df[alt], errors="coerce")
                if not dt.isna().all():
                    date_col = alt
                    break

    # If we *still* don't have a usable datetime, give up and return empty.
    if dt is None or dt.isna().all():
        if debug:
            print("EVENTS DEBUG — could not parse any usable date column")
        return pd.DataFrame(columns=["date","borough","event_count"])

    # Attach parsed datetime and drop rows where it is missing
    df = df.assign(_dt=dt).dropna(subset=["_dt"])

    # Local date window filter (robust, and independent of whether WHERE worked)
    mask = (df["_dt"] >= f"{start_date}T00:00:00") & (df["_dt"] <= f"{end_date}T23:59:59")
    df = df.loc[mask]
    if df.empty:
        return pd.DataFrame(columns=["date","borough","event_count"])

    # Borough normalization: try to use the detected borough column if present,
    # otherwise create a series of Nones (which will be dropped).
    if boro_col and boro_col in df.columns:
        b = _normalize_borough(df[boro_col])
    else:
        b = pd.Series([None]*len(df))

    # Final shape: one row per raw event, with clean date + borough
    df = df.assign(date=df["_dt"].dt.date, borough=b).drop(columns=["_dt"])
    df = df.dropna(subset=["borough"])
    if df.empty:
        return pd.DataFrame(columns=["date","borough","event_count"])

    # Group to daily counts: (date, borough) → event_count
    out = (
        df.groupby(["date","borough"], as_index=False)
          .size()
          .rename(columns={"size": "event_count"})
    )
    # Return tidy table used by the rest of the pipeline
    return out[["date","borough","event_count"]]


# -----------------------------------------------------------------------------
# PUBLIC ENTRYPOINT
# -----------------------------------------------------------------------------

def fetch_events_daily(start_date: str, end_date: str, app_token: Optional[str] = None) -> pd.DataFrame:
    """
    Public function used by the ETL pipeline.

    - Wires up HTTP headers (User-Agent + optional Socrata app token).
    - Calls `_fetch_events` to do the heavy lifting (paging, parsing, grouping).
    - Returns a clean DataFrame with columns: date, borough, event_count.

    > "This is the function my ETL calls to get a daily
    """
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    tok = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if tok:
        headers["X-App-Token"] = tok

    df = _fetch_events(NYC_EVENTS_URL, start_date, end_date, headers)

    # Optional debug logging if I want to inspect what came back
    if os.getenv("EVENTS_DEBUG") == "1":
        print(f"EVENTS DEBUG — final grouped rows: {len(df)}")
        if not df.empty:
            print(df.head(10).to_string(index=False))

    return df
