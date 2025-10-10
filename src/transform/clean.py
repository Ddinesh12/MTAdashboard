# src/transform/clean.py
from __future__ import annotations
import pandas as pd
import numpy as np

VALID_BOROUGHS = {"Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"}
BORO_MAP = {
    "MN": "Manhattan", "MANHATTAN": "Manhattan",
    "BX": "Bronx",     "BRONX": "Bronx",
    "BK": "Brooklyn",  "BKLN": "Brooklyn", "BROOKLYN": "Brooklyn",
    "QN": "Queens",    "QUEENS": "Queens",
    "SI": "Staten Island", "S.I.": "Staten Island",
    "STATEN ISLAND": "Staten Island", "STATENISLAND": "Staten Island",
}

def _ensure_date(col: pd.Series) -> pd.Series:
    return pd.to_datetime(col, errors="coerce").dt.date

def to_ridership_table(df: pd.DataFrame) -> pd.DataFrame:
    """Expecting columns: ['date','mode','riders','source'] from extractor."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","mode","riders","source"])

    out = df.copy()
    out["date"] = _ensure_date(out.get("date"))
    out["mode"] = out.get("mode", "subway").astype(str).str.lower()
    out["mode"] = out["mode"].where(out["mode"].isin(["subway","bus"]), "subway")
    out["riders"] = pd.to_numeric(out.get("riders"), errors="coerce").fillna(0).round().astype("Int64").clip(lower=0)

    if "source" in out.columns:
        out["source"] = out["source"].astype(str)
    else:
        out["source"] = "unknown"

    out = out[["date","mode","riders","source"]].dropna(subset=["date"])
    out = out.drop_duplicates(subset=["date","mode"]).reset_index(drop=True)
    return out

def to_weather_table(df: pd.DataFrame) -> pd.DataFrame:
    """Expecting: ['date','station_id','tmax_f','tmin_f','prcp_in','snow_in']."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","station_id","tmax_f","tmin_f","prcp_in","snow_in"])

    out = df.copy()
    out["date"] = _ensure_date(out.get("date"))
    out["station_id"] = out.get("station_id","USW00094728").astype(str)
    for c in ["tmax_f","tmin_f","prcp_in","snow_in"]:
        out[c] = pd.to_numeric(out.get(c), errors="coerce")

    out["tmax_f"] = out["tmax_f"].clip(lower=-30, upper=120)
    out["tmin_f"] = out["tmin_f"].clip(lower=-50, upper=100)
    out["prcp_in"] = out["prcp_in"].clip(lower=0)
    out["snow_in"] = out["snow_in"].clip(lower=0)

    out = out[["date","station_id","tmax_f","tmin_f","prcp_in","snow_in"]].dropna(subset=["date"])
    out = out.drop_duplicates(subset=["date"]).reset_index(drop=True)
    return out

def to_hourly_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expecting: ['date','hour','borough','riders','source'] from extractor.
    Tolerant borough normalization (maps MN/BX/BK/QN/SI etc. to full names).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","hour","borough","riders","source"])

    out = df.copy()
    out["date"] = _ensure_date(out.get("date"))
    out["hour"] = pd.to_numeric(out.get("hour"), errors="coerce").astype("Int64")
    out["hour"] = out["hour"].where((out["hour"] >= 0) & (out["hour"] <= 23))

    if "borough" in out:
        b = out["borough"].astype(str).str.strip()
        b = b.map(lambda s: BORO_MAP.get(s.upper(), s.title()))
        out["borough"] = b
    else:
        out["borough"] = pd.NA

    out["borough"] = out["borough"].where(out["borough"].isin(VALID_BOROUGHS))
    out["riders"]  = pd.to_numeric(out.get("riders"), errors="coerce").fillna(0).round().astype("Int64").clip(lower=0)

    if "source" in out.columns:
        out["source"] = out["source"].astype(str)
    else:
        # safe default for post-grouped hourly
        out["source"] = "data.ny.gov/hourly"

    out = out.dropna(subset=["date","hour","borough"])
    out = out[["date","hour","borough","riders","source"]]
    out = out.drop_duplicates(subset=["date","hour","borough"]).reset_index(drop=True)
    return out

def to_events_table(df: pd.DataFrame) -> pd.DataFrame:
    """Optional events normalization: expects ['date','borough','event_count']."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","borough","event_count"])

    out = df.copy()
    out["date"] = _ensure_date(out.get("date"))
    b = out.get("borough").astype(str).str.strip().map(lambda s: BORO_MAP.get(s.upper(), s.title()))
    out["borough"] = b.where(b.isin(VALID_BOROUGHS))
    out["event_count"] = pd.to_numeric(out.get("event_count"), errors="coerce").fillna(0).astype("Int64").clip(lower=0)

    out = out.dropna(subset=["date","borough"])
    out = out[["date","borough","event_count"]].drop_duplicates(subset=["date","borough"]).reset_index(drop=True)
    return out
