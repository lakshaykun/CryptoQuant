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
    open_time, close, volume, trades, taker_buy_base, symbol,
    log_return, volatility, imbalance_ratio, buy_ratio, vwap,
    log_return_lag1, log_return_lag2, buy_ratio_lag1,
    ma_5, ma_20, volatility_5, volume_5, buy_ratio_5,
    momentum, volume_spike, price_range_ratio, body_size,
    hour, day_of_week, trend_strength, volatility_ratio
"""

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "symbol",
]

OUTPUT_COLUMNS = [
    "open_time",
    "close",
    "volume",
    "trades",
    "taker_buy_base",
    "symbol",
    "log_return",
    "volatility",
    "imbalance_ratio",
    "buy_ratio",
    "vwap",
    "log_return_lag1",
    "log_return_lag2",
    "buy_ratio_lag1",
    "ma_5",
    "ma_20",
    "volatility_5",
    "volume_5",
    "buy_ratio_5",
    "momentum",
    "volume_spike",
    "price_range_ratio",
    "body_size",
    "hour",
    "day_of_week",
    "trend_strength",
    "volatility_ratio"
]


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, np.nan)
    return numerator / denominator


def _empty_output_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open_time": pd.Series(dtype="datetime64[ns]"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "trades": pd.Series(dtype="float64"),
            "taker_buy_base": pd.Series(dtype="float64"),
            "symbol": pd.Series(dtype="object"),
            "log_return": pd.Series(dtype="float64"),
            "volatility": pd.Series(dtype="float64"),
            "imbalance_ratio": pd.Series(dtype="float64"),
            "buy_ratio": pd.Series(dtype="float64"),
            "vwap": pd.Series(dtype="float64"),
            "log_return_lag1": pd.Series(dtype="float64"),
            "log_return_lag2": pd.Series(dtype="float64"),
            "buy_ratio_lag1": pd.Series(dtype="float64"),
            "ma_5": pd.Series(dtype="float64"),
            "ma_20": pd.Series(dtype="float64"),
            "volatility_5": pd.Series(dtype="float64"),
            "volume_5": pd.Series(dtype="float64"),
            "buy_ratio_5": pd.Series(dtype="float64"),
            "momentum": pd.Series(dtype="float64"),
            "volume_spike": pd.Series(dtype="float64"),
            "price_range_ratio": pd.Series(dtype="float64"),
            "body_size": pd.Series(dtype="float64"),
            "hour": pd.Series(dtype="float64"),
            "day_of_week": pd.Series(dtype="float64"),
            "trend_strength": pd.Series(dtype="float64"),
            "volatility_ratio": pd.Series(dtype="float64"),
        }
    )[OUTPUT_COLUMNS]


def add_base_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-bar derived features from raw OHLCV columns."""
    df = df.copy()
    price_ratio = _safe_divide(df["close"], df["open"])
    df["log_return"] = np.log(price_ratio.where(price_ratio > 0))
    df["volatility"] = df["high"] - df["low"]
    df["imbalance_ratio"] = _safe_divide(
        df["taker_buy_base"] - (df["volume"] - df["taker_buy_base"]),
        df["volume"],
    )
    df["buy_ratio"] = _safe_divide(df["taker_buy_base"], df["volume"])
    df["vwap"] = _safe_divide(df["quote_volume"], df["volume"])
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
    df["momentum"] = df["close"] - df["ma_5"]
    df["volume_spike"] = _safe_divide(df["volume"], df["volume_5"])
    df["price_range_ratio"] = _safe_divide(df["high"] - df["low"], df["close"])
    df["body_size"] = _safe_divide(df["close"] - df["open"], df["close"])
    df["trend_strength"] = df["ma_5"] - df["ma_20"]
    df["volatility_ratio"] = _safe_divide(df["volatility"], df["volatility_5"])
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract hour-of-day and day-of-week from open_time."""
    df = df.copy()
    ts = pd.to_datetime(df["open_time"], errors="coerce")
    df["hour"] = ts.dt.hour
    df["day_of_week"] = ts.dt.dayofweek   # Monday=0, Sunday=6
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
    if df.empty:
        return _empty_output_frame()

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = df.copy()
    df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")

    # Sort by symbol then time before any windowed operation.
    df = df.sort_values(["symbol", "open_time"]).reset_index(drop=True)

    df = add_base_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_derived_features(df)
    df = add_time_features(df)

    df = drop_raw_columns(df)
    df = drop_incomplete_rows(df)

    if df.empty:
        return _empty_output_frame()

    df = df.reindex(columns=OUTPUT_COLUMNS)

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