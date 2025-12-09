# src/transform/clean.py
from __future__ import annotations
import pandas as pd
import numpy as np

# Allowed borough names in the cleaned data
VALID_BOROUGHS = {"Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"}

# Map messy/raw borough codes/names into the clean set above
BORO_MAP = {
    "MN": "Manhattan", "MANHATTAN": "Manhattan",
    "BX": "Bronx",     "BRONX": "Bronx",
    "BK": "Brooklyn",  "BKLN": "Brooklyn", "BROOKLYN": "Brooklyn",
    "QN": "Queens",    "QUEENS": "Queens",
    "SI": "Staten Island", "S.I.": "Staten Island",
    "STATEN ISLAND": "Staten Island", "STATENISLAND": "Staten Island",
}

def _ensure_date(col: pd.Series) -> pd.Series:
    """
    Convert a Series to datetime, coerce errors, then take only the date part.

    This ensures we store plain Python date objects instead of full timestamps.
    """
    return pd.to_datetime(col, errors="coerce").dt.date


def to_ridership_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize daily ridership data.

    Expecting columns (from the extractor):
      ['date', 'mode', 'riders', 'source']

    Returns a DataFrame with:
      - date: date object
      - mode: 'subway' or 'bus'
      - riders: non-negative integer
      - source: string
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "mode", "riders", "source"])

    out = df.copy()

    # Ensure date is actual date, with invalid values turned into NaT/NaN
    out["date"] = _ensure_date(out.get("date"))

    # Normalize mode: default to "subway", lowercase, keep only subway/bus
    out["mode"] = out.get("mode", "subway").astype(str).str.lower()
    out["mode"] = out["mode"].where(out["mode"].isin(["subway", "bus"]), "subway")

    # Riders: numeric, fill missing with 0, round, Int64 type, no negatives
    out["riders"] = (
        pd.to_numeric(out.get("riders"), errors="coerce")
        .fillna(0)
        .round()
        .astype("Int64")
        .clip(lower=0)
    )

    # Source: keep as string if present, otherwise set to "unknown"
    if "source" in out.columns:
        out["source"] = out["source"].astype(str)
    else:
        out["source"] = "unknown"

    # Keep only relevant columns and drop rows with missing date
    out = out[["date", "mode", "riders", "source"]].dropna(subset=["date"])

    # Drop duplicate (date, mode) pairs, keep first occurrence
    out = out.drop_duplicates(subset=["date", "mode"]).reset_index(drop=True)
    return out


def to_weather_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize daily weather data.

    Expecting columns (from the extractor):
      ['date', 'station_id', 'tmax_f', 'tmin_f', 'prcp_in', 'snow_in']

    Returns a DataFrame with one row per date and reasonable value ranges.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "station_id", "tmax_f", "tmin_f", "prcp_in", "snow_in"])

    out = df.copy()

    # Ensure date is actual date
    out["date"] = _ensure_date(out.get("date"))

    # Station ID: default to Central Park station if missing
    out["station_id"] = out.get("station_id", "USW00094728").astype(str)

    # Convert numeric weather fields to numeric
    for c in ["tmax_f", "tmin_f", "prcp_in", "snow_in"]:
        out[c] = pd.to_numeric(out.get(c), errors="coerce")

    # Apply simple sanity bounds to avoid insane outliers
    out["tmax_f"] = out["tmax_f"].clip(lower=-30, upper=120)
    out["tmin_f"] = out["tmin_f"].clip(lower=-50, upper=100)
    out["prcp_in"] = out["prcp_in"].clip(lower=0)
    out["snow_in"] = out["snow_in"].clip(lower=0)

    # Keep only relevant columns, drop rows with no date
    out = out[["date", "station_id", "tmax_f", "tmin_f", "prcp_in", "snow_in"]].dropna(subset=["date"])

    # Enforce one row per date (drop duplicates)
    out = out.drop_duplicates(subset=["date"]).reset_index(drop=True)
    return out


def to_hourly_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize hourly ridership data.

    Expecting (from extractor):
      ['date', 'hour', 'borough', 'riders', 'source']

    - Normalizes date and hour (0–23).
    - Normalizes boroughs using BORO_MAP and VALID_BOROUGHS.
    - Ensures riders is non-negative integer.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "source"])

    out = df.copy()

    # Date column cleaned to date objects
    out["date"] = _ensure_date(out.get("date"))

    # Hour to Int64, keep only valid 0–23 values
    out["hour"] = pd.to_numeric(out.get("hour"), errors="coerce").astype("Int64")
    out["hour"] = out["hour"].where((out["hour"] >= 0) & (out["hour"] <= 23))

    # Borough normalization: map codes/names into clean labels
    if "borough" in out:
        b = out["borough"].astype(str).str.strip()
        # Map using BORO_MAP first, otherwise title-case the string
        b = b.map(lambda s: BORO_MAP.get(s.upper(), s.title()))
        out["borough"] = b
    else:
        # If no borough column at all, fill with NA (will be dropped later)
        out["borough"] = pd.NA

    # Keep only boroughs that are part of our valid set
    out["borough"] = out["borough"].where(out["borough"].isin(VALID_BOROUGHS))

    # Riders: numeric, fill missing with 0, round, Int64 type, no negatives
    out["riders"] = (
        pd.to_numeric(out.get("riders"), errors="coerce")
        .fillna(0)
        .round()
        .astype("Int64")
        .clip(lower=0)
    )

    # Source: keep existing if present, otherwise apply a sensible default
    if "source" in out.columns:
        out["source"] = out["source"].astype(str)
    else:
        # Default label for “generic hourly source”
        out["source"] = "data.ny.gov/hourly"

    # Drop rows missing any of date/hour/borough (these are required keys)
    out = out.dropna(subset=["date", "hour", "borough"])

    # Keep only relevant columns
    out = out[["date", "hour", "borough", "riders", "source"]]

    # Remove duplicate (date, hour, borough) combinations
    out = out.drop_duplicates(subset=["date", "hour", "borough"]).reset_index(drop=True)
    return out


def to_events_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize daily event data.

    Expecting:
      ['date', 'borough', 'event_count']

    Returns:
      one row per (date, borough) with a non-negative event_count.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "borough", "event_count"])

    out = df.copy()

    # Ensure date is a date object
    out["date"] = _ensure_date(out.get("date"))

    # Borough normalization: map codes/names and keep only valid boroughs
    b = (
        out.get("borough")
        .astype(str)
        .str.strip()
        .map(lambda s: BORO_MAP.get(s.upper(), s.title()))
    )
    out["borough"] = b.where(b.isin(VALID_BOROUGHS))

    # Event count: numeric, fill missing with 0, integer, no negatives
    out["event_count"] = (
        pd.to_numeric(out.get("event_count"), errors="coerce")
        .fillna(0)
        .astype("Int64")
        .clip(lower=0)
    )

    # Drop rows with missing date or borough (required dimensions)
    out = out.dropna(subset=["date", "borough"])

    # Keep only the final columns and drop duplicate (date, borough) pairs
    out = (
        out[["date", "borough", "event_count"]]
        .drop_duplicates(subset=["date", "borough"])
        .reset_index(drop=True)
    )
    return out
