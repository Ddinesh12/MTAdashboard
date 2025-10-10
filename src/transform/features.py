# src/transform/features.py
from __future__ import annotations

import pandas as pd
import numpy as np

def add_weather_flags(weather: pd.DataFrame) -> pd.DataFrame:
    if weather is None or weather.empty:
        return pd.DataFrame(columns=["date","tmax_f","prcp_in","wet_day","hot_day","cold_day"])
    w = weather.copy()
    w["wet_day"] = (w["prcp_in"].astype(float) > 0)
    w["hot_day"] = (w["tmax_f"].astype(float) >= 85)
    w["cold_day"] = (w["tmax_f"].astype(float) <= 32)
    return w

def join_ridership_weather(
    ridership_daily: pd.DataFrame,
    weather_daily: pd.DataFrame,
    events_daily: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if ridership_daily is None or ridership_daily.empty:
        return pd.DataFrame(columns=["date","mode","riders"])
    w = add_weather_flags(weather_daily) if weather_daily is not None else None

    out = ridership_daily.copy()
    if w is not None and not w.empty:
        out = out.merge(
            w[["date","tmax_f","prcp_in","wet_day","hot_day","cold_day"]],
            on="date", how="left"
        )
    if events_daily is not None and not events_daily.empty:
        # collapse borough → systemwide per day
        e = events_daily.groupby("date", as_index=False)["event_count"].sum()
        out = out.merge(e, on="date", how="left")
    else:
        out["event_count"] = np.nan
    return out

def add_rolling_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 7d/28d moving averages and a 180d trailing baseline by mode.
    """
    if df is None or df.empty:
        return df
    out = df.sort_values(["mode","date"]).copy()
    out["riders_ma7"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
    )
    out["riders_ma28"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.rolling(window=28, min_periods=7).mean())
    )
    # 180d baseline excluding current day
    out["riders_baseline_180"] = (
        out.groupby("mode")["riders"]
        .transform(lambda s: s.shift(1).rolling(window=180, min_periods=28).mean())
    )
    # percent delta vs baseline
    out["pct_delta_vs_180"] = np.where(
        out["riders_baseline_180"].fillna(0) == 0, np.nan,
        (out["riders"] - out["riders_baseline_180"]) / out["riders_baseline_180"]
    )
    return out

def rush_hour_multiplier(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Returns per-day, per-borough rush hour multiplier = peak_hour / avg_hour.
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date","borough","rush_hour_multiplier","peak_hourly","avg_hourly"])
    h = hourly.copy()
    daily = (
        h.groupby(["date","borough"], as_index=False)["riders"]
         .agg(avg_hourly=("riders","mean"), peak_hourly=("riders","max"))
    )
    daily["rush_hour_multiplier"] = np.where(
        daily["avg_hourly"] == 0, np.nan, daily["peak_hourly"] / daily["avg_hourly"]
    )
    return daily[["date","borough","rush_hour_multiplier","peak_hourly","avg_hourly"]]

def weekend_factor(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Returns per-hour, per-borough weekend_factor = avg(weekend) / avg(weekday)
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["borough","hour","weekend_factor","weekend_avg","weekday_avg"])
    h = hourly.copy()
    h["dow"] = pd.to_datetime(h["date"]).dt.weekday  # 0=Mon..6=Sun
    h["is_weekend"] = h["dow"].isin([5,6])
    split = (
        h.groupby(["borough","hour","is_weekend"])["riders"]
         .mean().unstack("is_weekend").rename(columns={False:"weekday_avg", True:"weekend_avg"})
    ).reset_index()
    split["weekend_factor"] = np.where(
        split["weekday_avg"].fillna(0)==0, np.nan, split["weekend_avg"]/split["weekday_avg"]
    )
    return split[["borough","hour","weekend_factor","weekend_avg","weekday_avg"]]

def hourly_anomalies(hourly: pd.DataFrame, window: int = 28) -> pd.DataFrame:
    """
    Z-score anomalies per (borough, hour) vs rolling window (excluding current day).
    """
    if hourly is None or hourly.empty:
        return pd.DataFrame(columns=["date","hour","borough","riders","zscore"])
    h = hourly.sort_values(["borough","hour","date"]).copy()
    def _z(s: pd.Series) -> pd.Series:
        m = s.shift(1).rolling(window=window, min_periods=max(7, window//4)).mean()
        sd = s.shift(1).rolling(window=window, min_periods=max(7, window//4)).std()
        return (s - m) / sd.replace(0, np.nan)
    h["zscore"] = h.groupby(["borough","hour"])["riders"].transform(_z)
    return h[["date","hour","borough","riders","zscore"]]
