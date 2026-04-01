"""
build_features.py
-----------------------
Transforms a raw BTC OHLCV pandas DataFrame (Bronze layer schema) into a
feature-rich Silver layer DataFrame ready for model training.

Expected input columns
----------------------
    open_time, open, high, low, close, volume,
    close_time, quote_volume, trades,
    taker_buy_base, taker_buy_quote, symbol

Output columns
--------------
    timestamp, close, volume, trades, taker_buy_base, symbol,
    log_return, volatility, imbalance_ratio, buy_ratio, vwap,
    log_return_lag1, log_return_lag2, buy_ratio_lag1,
    ma_5, ma_20, volatility_5, volume_5, buy_ratio_5,
    momentum, volume_spike, price_range_ratio, body_size,
    hour, day_of_week, trend_strength, volatility_ratio,
    target
"""

import numpy as np
import pandas as pd


def add_base_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-bar derived features from raw OHLCV columns."""
    df = df.copy()
    df["log_return"]       = np.log(df["close"] / df["open"])
    df["volatility"]       = df["high"] - df["low"]
    df["imbalance_ratio"]  = (
        df["taker_buy_base"] - (df["volume"] - df["taker_buy_base"])
    ) / df["volume"]
    df["buy_ratio"]        = df["taker_buy_base"] / df["volume"]
    df["vwap"]             = df["quote_volume"] / df["volume"]
    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add 1- and 2-bar lags for log_return and a 1-bar lag for buy_ratio."""
    df = df.copy()
    grp = df.groupby("symbol")
    df["log_return_lag1"] = grp["log_return"].shift(1)
    df["log_return_lag2"] = grp["log_return"].shift(2)
    df["buy_ratio_lag1"]  = grp["buy_ratio"].shift(1)
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling averages over 5-bar and 20-bar windows per symbol.

    Window sizes match the notebook's PySpark rowsBetween(-N, 0):
      - 5-bar  → window=6  (current row + 5 prior)
      - 20-bar → window=21 (current row + 20 prior)
    """
    df = df.copy()

    def roll(series: pd.Series, window: int) -> pd.Series:
        return series.rolling(window=window, min_periods=1).mean()

    for symbol, group in df.groupby("symbol"):
        idx = group.index
        df.loc[idx, "ma_5"]         = roll(group["close"],      6)
        df.loc[idx, "ma_20"]        = roll(group["close"],     21)
        df.loc[idx, "volatility_5"] = roll(group["volatility"], 6)
        df.loc[idx, "volume_5"]     = roll(group["volume"],     6)
        df.loc[idx, "buy_ratio_5"]  = roll(group["buy_ratio"],  6)

    return df


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute higher-order features that depend on base / rolling features."""
    df = df.copy()
    df["momentum"]          = df["close"] - df["ma_5"]
    df["volume_spike"]      = df["volume"] / df["volume_5"]
    df["price_range_ratio"] = (df["high"] - df["low"]) / df["close"]
    df["body_size"]         = (df["close"] - df["open"]) / df["close"]
    df["trend_strength"]    = df["ma_5"] - df["ma_20"]
    df["volatility_ratio"]  = df["volatility"] / df["volatility_5"]
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract hour-of-day and day-of-week from open_time."""
    df = df.copy()
    ts               = pd.to_datetime(df["open_time"])
    df["hour"]        = ts.dt.hour
    df["day_of_week"] = ts.dt.dayofweek   # Monday=0, Sunday=6
    return df


def add_target(df: pd.DataFrame) -> pd.DataFrame:
    """Add the prediction target: next bar's log_return (lead by 1 per symbol)."""
    df = df.copy()
    df["target"] = df.groupby("symbol")["log_return"].shift(-1)
    return df


def drop_raw_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns that are not needed downstream."""
    cols_to_drop = ["quote_volume", "taker_buy_quote", "open", "high", "low", "close_time"]
    return df.drop(columns=[c for c in cols_to_drop if c in df.columns])


def drop_incomplete_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop warm-up rows where lag / rolling features are not yet populated."""
    required = ["log_return_lag1", "log_return_lag2", "buy_ratio_lag1", "ma_5", "ma_20"]
    return df.dropna(subset=required).reset_index(drop=True)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full feature engineering pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        Raw Bronze-layer DataFrame with columns:
        open_time, open, high, low, close, volume, close_time,
        quote_volume, trades, taker_buy_base, taker_buy_quote, symbol

    Returns
    -------
    pd.DataFrame
        Silver-layer DataFrame with all engineered features and target column.
    """
    # Sort by symbol then time before any windowed operation
    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

    df = add_base_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_derived_features(df)
    df = add_time_features(df)
    df = add_target(df)

    df = drop_raw_columns(df)
    df = drop_incomplete_rows(df)

    df = df.rename(columns={"open_time": "timestamp"})

    return df


# ---------------------------------------------------------------------------
# Quick smoke-test  —  python build_features.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os

    CSV_PATH = os.getenv("CSV_PATH", "../../datasets/btc_data.csv")

    raw_df = pd.read_csv(CSV_PATH, parse_dates=["open_time", "close_time"])

    # Mirror the notebook's bronze ingestion filter
    raw_df = raw_df[raw_df["open_time"] >= "2025-01-01"].copy()
    raw_df["symbol"] = "BTCUSDT"

    silver_df = build_features(raw_df)

    print(silver_df.head())
    print("\nOutput columns:", silver_df.columns.tolist())
    print(f"Shape: {silver_df.shape}")