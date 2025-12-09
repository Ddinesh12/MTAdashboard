# src/transform/features.py
from __future__ import annotations

import pandas as pd
import numpy as np

def add_weather_flags(weather: pd.DataFrame) -> pd.DataFrame:
    """
    Add simple boolean flags to daily weather data:
      - wet_day: any precipitation > 0
      - hot_day: max temp >= 85 F
      - cold_day: max temp <= 32 F
    """
    if weather is None or weather.empty:
        return pd.DataFrame(columns=["date", "tmax_f", "prcp_in", "wet_day", "hot_day", "cold_day"])

    # Work on a copy so we don't modify the original DataFrame
    w = weather.copy()

    # True if there was any measurable precipitation that day
    w["wet_day"] = (w["prcp_in"].astype(float) > 0)

    # True if the high temperature is at or above a "hot" threshold
    w["hot_day"] = (w["tmax_f"].astype(float) >= 85)

    # True if the high temperature is at or below freezing
    w["cold_day"] = (w["tmax_f"].astype(float) <= 32)

    return w


def join_ridership_weather(
    ridership_daily: pd.DataFrame,
    weather_daily: pd.DataFrame,
    events_daily: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Join daily ridership with weather (and optional events) on the date column.

    - ridership_daily: daily ridership by mode
    - weather_daily: daily weather (tmax, precip, etc.)
    - events_daily: optional daily event counts by borough

    Output is ridership with extra columns:
      tmax_f, prcp_in, wet_day, hot_day, cold_day, event_count
    """
    if ridership_daily is None or ridership_daily.empty:
        return pd.DataFrame(columns=["date", "mode", "riders"])

    # Add wet/hot/cold flags to weather if we have weather data
    w = add_weather_flags(weather_daily) if weather_daily is not None else None

    out = ridership_daily.copy()

    # Left join weather info by date (keep all ridership rows)
    if w is not None and not w.empty:
        out = out.merge(
            w[["date", "tmax_f", "prcp_in", "wet_day", "hot_day", "cold_day"]],
            on="date",
            how="left",
        )

    # Optionally add systemwide event counts (summed over boroughs)
    if events_daily is not None and not events_daily.empty:
        # Aggregate events from (date, borough) to (date) total event_count
        e = events_daily.groupby("date", as_index=False)["event_count"].sum()
        out = out.merge(e, on="date", how="left")
    else:
        # If no events provided, fill event_count with NaN
        out["event_count"] = np.nan

    return out


def add_rolling_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling metrics to daily ridership by mode:

      - riders_ma7:  7-day moving average (min 1 day)
      - riders_ma28: 28-day moving average (min 7 days)
      - riders_baseline_180: 180-day trailing average excluding current day
      - pct_delta_vs_180: percent difference vs the 180-day baseline
    """
    if df is None or df.empty:
        return df

    # Sort by mode + date so rolling windows are applied in time order per mode
    out = df.sort_values(["mode", "date"]).copy()

    # 7-day moving average of riders, per mode (includes current day)
    out["riders_ma7"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
    )

    # 28-day moving average of riders, per mode (requires at least 7 days)
    out["riders_ma28"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.rolling(window=28, min_periods=7).mean())
    )

    # 180-day baseline: average of the previous 180 days, excluding today
    # (shift(1) moves the window back by one day so we don't include today)
    out["riders_baseline_180"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.shift(1).rolling(window=180, min_periods=28).mean())
    )

    # Percent delta vs the 180-day baseline:
    #   (today - baseline) / baseline
    # If baseline is 0 or NaN, we set result to NaN.
    out["pct_delta_vs_180"] = np.where(
        out["riders_baseline_180"].fillna(0) == 0,
        np.nan,
        (out["riders"] - out["riders_baseline_180"]) / out["riders_baseline_180"],
    )

    return out


def rush_hour_multiplier(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-day, per-borough rush hour multiplier:

      rush_hour_multiplier = peak_hourly / avg_hourly

    where:
      - avg_hourly  = mean hourly riders for that day+borough
      - peak_hourly = max hourly riders for that day+borough
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "borough", "rush_hour_multiplier", "peak_hourly", "avg_hourly"])

    h = hourly.copy()

    # Aggregate hourly data to daily averages and peaks per borough
    daily = (
        h.groupby(["date", "borough"], as_index=False)["riders"]
         .agg(avg_hourly=("riders", "mean"), peak_hourly=("riders", "max"))
    )

    # Compute peak / avg, guard against division by zero
    daily["rush_hour_multiplier"] = np.where(
        daily["avg_hourly"] == 0,
        np.nan,
        daily["peak_hourly"] / daily["avg_hourly"],
    )

    return daily[["date", "borough", "rush_hour_multiplier", "peak_hourly", "avg_hourly"]]


def weekend_factor(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-hour, per-borough weekend_factor:

      weekend_factor = average weekend ridership / average weekday ridership

    Output columns:
      - borough
      - hour
      - weekend_factor
      - weekend_avg
      - weekday_avg
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["borough", "hour", "weekend_factor", "weekend_avg", "weekday_avg"])

    h = hourly.copy()

    # Day-of-week: 0=Mon, ..., 6=Sun
    h["dow"] = pd.to_datetime(h["date"]).dt.weekday

    # Weekend flag: True for Saturday (5) and Sunday (6)
    h["is_weekend"] = h["dow"].isin([5, 6])

    # Group by borough, hour, weekend/weekday, then compute mean riders
    split = (
        h.groupby(["borough", "hour", "is_weekend"])["riders"]
         .mean()
         .unstack("is_weekend")  # columns: {False: weekday, True: weekend}
         .rename(columns={False: "weekday_avg", True: "weekend_avg"})
    ).reset_index()

    # weekend_factor = weekend_avg / weekday_avg (NaN if weekday_avg is 0 or missing)
    split["weekend_factor"] = np.where(
        split["weekday_avg"].fillna(0) == 0,
        np.nan,
        split["weekend_avg"] / split["weekday_avg"],
    )

    return split[["borough", "hour", "weekend_factor", "weekend_avg", "weekday_avg"]]


def hourly_anomalies(hourly: pd.DataFrame, window: int = 28) -> pd.DataFrame:
    """
    Compute Z-score anomalies for hourly riders per (borough, hour).

    For each borough+hour timeseries, we:
      - Look at the last `window` days (excluding today via shift(1)).
      - Compute rolling mean and std.
      - Z-score = (today - rolling_mean) / rolling_std.

    Higher |zscore| values indicate more unusual ridership for that borough+hour.
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date", "hour", "borough", "riders", "zscore"])

    # Sort so each (borough, hour) group is in date order
    h = hourly.sort_values(["borough", "hour", "date"]).copy()

    def _z(s: pd.Series) -> pd.Series:
        """
        Helper: compute rolling Z-score for a 1D series of riders.

        - Uses shifted rolling mean/std over `window` days so we do not
          include the current day in its own baseline.
        - min_periods = max(7, window//4) to avoid tiny windows.
        """
        # Rolling mean and std over the past `window` values, excluding current one
        m = s.shift(1).rolling(window=window, min_periods=max(7, window // 4)).mean()
        sd = s.shift(1).rolling(window=window, min_periods=max(7, window // 4)).std()
        # Avoid division by zero by replacing std=0 with NaN
        return (s - m) / sd.replace(0, np.nan)

    # Apply Z-score calculation per (borough, hour) series
    h["zscore"] = h.groupby(["borough", "hour"])["riders"].transform(_z)

    return h[["date", "hour", "borough", "riders", "zscore"]]
