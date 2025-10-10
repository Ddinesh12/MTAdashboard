# src/extract/events_daily.py
from __future__ import annotations
import os, time
from typing import Dict, Any, List, Optional
import pandas as pd
import requests

# NYC Permitted Event Information (correct dataset)
NYC_EVENTS_URL = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"

DEFAULT_USER_AGENT = "nyc-mta-dashboard/0.1 (educational project)"
VALID_BOROUGHS = {"Bronx","Brooklyn","Manhattan","Queens","Staten Island"}
BORO_MAP = {
    "MN": "Manhattan", "MANHATTAN": "Manhattan",
    "BX": "Bronx",     "BRONX": "Bronx",
    "BK": "Brooklyn",  "BKLN": "Brooklyn", "BROOKLYN": "Brooklyn",
    "QN": "Queens",    "QUEENS": "Queens",
    "SI": "Staten Island", "S.I.": "Staten Island",
    "STATEN ISLAND": "Staten Island", "STATENISLAND": "Staten Island",
}

# We will pick exactly ONE of these for WHERE/ORDER (the first that exists).
PREFERRED_DATE_COLS = [
    "startdatetime",       # often present as a full timestamp
    "start_date",          # sometimes a (date or timestamp) field
    "event_date",          # date-only in some rows
    "begin_date",          # alt naming sometimes seen
    "starttime",           # time-only paired with date (rare)
    "date",                # generic
]

BORO_CANDS = [
    "borough", "event_borough", "borough_name", "boroname",
    "borocode", "borough_desc", "boroughdescription"
]

def _http_get(url: str, params: Dict[str, Any], headers: Dict[str, str], max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * attempt)
            continue
        r.raise_for_status()
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def _pick(cands: List[str], cols: List[str]) -> Optional[str]:
    for c in cands:
        if c in cols: 
            return c
    # very loose fallback
    for c in cols:
        lc = c.lower()
        for k in cands:
            if k in lc:
                return c
    return None

def _normalize_borough(series: pd.Series) -> pd.Series:
    b = series.astype(str).str.strip()
    b = b.map(lambda s: BORO_MAP.get(s.upper(), s.title()))
    return b.where(b.isin(VALID_BOROUGHS))

def _detect_columns(url: str, headers: Dict[str, str]) -> List[str]:
    # fetch 1 row to see actual field names
    probe = _http_get(url, {"$limit": 1}, headers)
    if isinstance(probe, list) and probe:
        return list(probe[0].keys())
    return []

def _safe_between(field: str, start_date: str, end_date: str) -> str:
    # Use datetime literal if likely a timestamp field, else date literal.
    fld = field.lower()
    if "time" in fld or "datetime" in fld:
        return f"{field} between '{start_date}T00:00:00' and '{end_date}T23:59:59'"
    return f"{field} between '{start_date}' and '{end_date}'"

def _fetch_events(url: str, start_date: str, end_date: str, headers: Dict[str,str]) -> pd.DataFrame:
    debug = os.getenv("EVENTS_DEBUG") == "1"

    # 1) detect real columns, then pick ONE date column that exists
    cols = _detect_columns(url, headers)
    if debug:
        print("EVENTS DEBUG — detected columns (first row):", cols[:40])

    date_col = _pick(PREFERRED_DATE_COLS, cols)
    boro_col = _pick(BORO_CANDS, cols)
    if not date_col:
        # As a last resort, accept any column containing 'date'
        for c in cols:
            if "date" in c.lower():
                date_col = c
                break

    # 2) Try to page using ONLY that date column in WHERE/ORDER
    rows: List[Dict[str, Any]] = []
    limit, offset, page = 50000, 0, 0
    params_base = {"$select": "*", "$limit": limit}
    where_ok = bool(date_col)

    while True:
        params = dict(params_base)
        params["$offset"] = offset
        if where_ok:
            params["$where"] = _safe_between(date_col, start_date, end_date)
            params["$order"] = date_col
        try:
            chunk = _http_get(url, params, headers)
        except requests.HTTPError:
            # If WHERE fails for any reason, disable and pull broadly, filter locally.
            if where_ok:
                where_ok = False
                continue
            else:
                raise

        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
        page += 1
        if page > 4:  # smoke safety
            break

    if debug:
        print(f"EVENTS DEBUG — pulled raw rows: {len(rows)} (where_ok={where_ok}, date_col={date_col})")

    if not rows:
        return pd.DataFrame(columns=["date","borough","event_count"])

    df = pd.DataFrame.from_records(rows)

    # 3) Build a usable datetime for local filtering
    dt = None
    if date_col and date_col in df.columns:
        dt = pd.to_datetime(df[date_col], errors="coerce")
    # fallback: try a commonly seen alternate field
    if dt is None or dt.isna().all():
        for alt in ["start_date", "startdatetime", "event_date", "date"]:
            if alt in df.columns:
                dt = pd.to_datetime(df[alt], errors="coerce")
                if not dt.isna().all():
                    date_col = alt
                    break

    if dt is None or dt.isna().all():
        if debug:
            print("EVENTS DEBUG — could not parse any usable date column")
        return pd.DataFrame(columns=["date","borough","event_count"])

    df = df.assign(_dt=dt).dropna(subset=["_dt"])

    # Local date window filter (robust)
    mask = (df["_dt"] >= f"{start_date}T00:00:00") & (df["_dt"] <= f"{end_date}T23:59:59")
    df = df.loc[mask]
    if df.empty:
        return pd.DataFrame(columns=["date","borough","event_count"])

    # Borough normalization
    if boro_col and boro_col in df.columns:
        b = _normalize_borough(df[boro_col])
    else:
        b = pd.Series([None]*len(df))
    df = df.assign(date=df["_dt"].dt.date, borough=b).drop(columns=["_dt"])
    df = df.dropna(subset=["borough"])
    if df.empty:
        return pd.DataFrame(columns=["date","borough","event_count"])

    out = (
        df.groupby(["date","borough"], as_index=False)
          .size()
          .rename(columns={"size": "event_count"})
    )
    return out[["date","borough","event_count"]]

def fetch_events_daily(start_date: str, end_date: str, app_token: Optional[str] = None) -> pd.DataFrame:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    tok = app_token or os.getenv("SOCRATA_APP_TOKEN")
    if tok:
        headers["X-App-Token"] = tok

    df = _fetch_events(NYC_EVENTS_URL, start_date, end_date, headers)
    if os.getenv("EVENTS_DEBUG") == "1":
        print(f"EVENTS DEBUG — final grouped rows: {len(df)}")
        if not df.empty:
            print(df.head(10).to_string(index=False))
    return df
