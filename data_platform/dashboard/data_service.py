"""
data_service.py – unified data layer for the CryptoQuant observability dashboard.

All heavy I/O goes through Streamlit @st.cache_data so repeated tab switches are free.
Key design decisions:
  - Prediction ↔ actual alignment uses a LEAD shift on gold data (target = t+1 close)
    rather than a join-by-timestamp that causes off-by-one leakage.
  - Regime columns (volatility_regime, volume_spike, imbalance_ratio) are preserved
    so callers can filter by market regime without reloading.
  - Training baseline stats are loaded from the parquet artifact that the training
    pipeline writes, not recomputed from live data, ensuring train/live comparison
    is apples-to-apples.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

from delta_client import load_delta_table
from helpers import resolve_project_path, to_utc_datetime, safe_float


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _coerce_numeric_cols(frame: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _align_prediction_to_actual(
    predictions: pd.DataFrame,
    gold: pd.DataFrame,
    symbol: str,
) -> pd.DataFrame:
    """
    Fix the prediction-time ↔ actual-label alignment bug.

    The model predicts log_return_lead1 at time T, meaning the actual outcome
    is the return from close[T] → close[T+1].  We therefore must shift the
    gold `close` series by -1 to get actual_close and recompute the log-return
    from there.  Joining on open_time directly without the lead shift produces
    an off-by-one error that makes the model look worse than it is.
    """
    if predictions.empty or gold.empty:
        return predictions

    g = gold[gold["symbol"] == symbol][["symbol", "open_time", "close"]].copy()
    g = g.sort_values("open_time").drop_duplicates("open_time")

    # Proper lead alignment: actual close at T+1
    g["actual_close"] = g["close"].shift(-1)
    g["log_return_now"] = np.log(g["close"] / g["close"].shift(1))
    g["actual_log_return_lead1"] = g["log_return_now"].shift(-1)

    g = g[["symbol", "open_time", "close", "actual_close", "actual_log_return_lead1"]]

    merged = predictions.drop(
        columns=[c for c in ["actual_close", "actual_log_return_lead1", "close"] if c in predictions.columns],
        errors="ignore",
    ).merge(g, on=["symbol", "open_time"], how="left")

    # Derived columns
    if "return_short" in merged.columns:
        merged["prediction"] = pd.to_numeric(merged["return_short"], errors="coerce")
    elif "prediction" not in merged.columns:
        merged["prediction"] = np.nan

    merged["predicted_close"] = merged["close"] * np.exp(merged["prediction"].fillna(0))
    merged["residual"] = merged["prediction"] - merged["actual_log_return_lead1"]
    merged["abs_error"] = merged["residual"].abs()

    # Direction match
    pred_sign = np.sign(merged["prediction"].fillna(0))
    actual_sign = np.sign(merged["actual_log_return_lead1"].fillna(0))
    merged["direction_correct"] = (pred_sign == actual_sign).astype(float)
    merged.loc[actual_sign == 0, "direction_correct"] = np.nan  # flat candle – skip

    return merged


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=30)
def get_predictions(
    data_config: Dict[str, Any],
    model_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    model_version: str,
    refresh_nonce: int,
    repo_root: str,
) -> pd.DataFrame:
    """Load prediction rows and align them to actual outcomes (lead-shifted)."""
    predictions_path = data_config.get("tables", {}).get("predictions_log_return_lead1", {}).get("path", "")
    pred_cols = [
        "open_time", "symbol", "close", "prediction",
        "return_short", "return_long", "sign_short", "sign_long",
        "log_return", "model_version",
    ]
    raw = load_delta_table(
        table_path=predictions_path,
        columns=pred_cols,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )
    if raw.empty:
        return raw

    if "model_version" in raw.columns and model_version != "All":
        raw = raw[raw["model_version"].astype(str) == model_version]

    # Load gold for alignment
    gold = _load_gold_minimal(data_config, symbol, start, end, refresh_nonce, repo_root)
    aligned = _align_prediction_to_actual(raw, gold, symbol)
    return aligned.dropna(subset=["prediction"]).copy()


@st.cache_data(show_spinner=False, ttl=30)
def get_actuals_with_lag_alignment(
    data_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    refresh_nonce: int,
    repo_root: str,
) -> pd.DataFrame:
    """Return gold rows with properly lead-shifted actual targets."""
    gold = _load_gold_minimal(data_config, symbol, start, end, refresh_nonce, repo_root)
    if gold.empty:
        return gold

    g = gold.sort_values("open_time").copy()
    g["actual_close"] = g["close"].shift(-1)
    g["log_return_now"] = np.log(g["close"] / g["close"].shift(1))
    g["actual_log_return_lead1"] = g["log_return_now"].shift(-1)
    return g


@st.cache_data(show_spinner=False, ttl=30)
def get_feature_snapshot(
    data_config: Dict[str, Any],
    model_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    refresh_nonce: int,
    repo_root: str,
) -> pd.DataFrame:
    """Load gold feature rows (all expected columns)."""
    gold_path = data_config.get("tables", {}).get("gold_market", {}).get("path", "")
    feature_cols = [
        f for f in model_config.get(
            "features_long", model_config.get("features_short", model_config.get("features", []))
        ) if f != "symbol"
    ]
    all_cols = list(dict.fromkeys([
        "open_time", "ingestion_time", "symbol", "close", "log_return", "date",
        "is_valid_feature_row", *feature_cols,
    ]))
    return load_delta_table(
        table_path=gold_path,
        columns=all_cols,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )


@st.cache_data(show_spinner=False, ttl=120)
def get_training_baseline_stats(model_config: Dict[str, Any], repo_root: str) -> pd.DataFrame:
    """
    Load mean/std statistics from the training parquet artifact.
    Returns a DataFrame with index = feature name, columns = [mean, std, min, max, p25, p75].
    Falls back to empty DataFrame if artifact not found.
    """
    train_path_raw = model_config.get("train_data_path", "")
    if not train_path_raw:
        return pd.DataFrame()

    resolved = resolve_project_path(train_path_raw, Path(repo_root))
    if not resolved.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(resolved)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        stats = df[numeric_cols].describe(percentiles=[0.25, 0.75]).T
        stats = stats.rename(columns={"25%": "p25", "75%": "p75"})
        return stats[["mean", "std", "min", "p25", "p75", "max"]]
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=30)
def get_drift_history(
    model_config: Dict[str, Any],
    start: datetime,
    end: datetime,
    refresh_nonce: int,
    repo_root: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (drift_summary, drift_features).
    drift_summary has __overall__ rows; drift_features has per-feature rows.
    """
    drift_path = model_config.get("monitoring", {}).get("history", {}).get("path", "")
    drift_cols = [
        "timestamp", "event_time", "feature_name", "drift_type",
        "drift_score", "drift_detected", "model_metric",
        "overall_drift_score", "data_drift_score", "model_drift_score",
        "triggered", "trigger_reason",
    ]
    raw = load_delta_table(
        table_path=drift_path,
        columns=drift_cols,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    time_col = "timestamp" if "timestamp" in raw.columns else "event_time"
    if time_col in raw.columns:
        raw[time_col] = to_utc_datetime(raw[time_col])

    if "feature_name" in raw.columns:
        summary = raw[raw["feature_name"] == "__overall__"].copy()
        features = raw[~raw["feature_name"].isin(["__overall__", "__model__"])].copy()
    else:
        summary = raw.copy()
        features = pd.DataFrame()

    if summary.empty:
        summary = raw.copy()

    # Normalise column names
    for df in [summary, features]:
        if time_col in df.columns and time_col != "event_time":
            df.rename(columns={time_col: "event_time"}, inplace=True)

    if "overall_drift_score" in summary.columns:
        summary["drift_score"] = pd.to_numeric(summary["overall_drift_score"], errors="coerce")

    if "prediction_drift_score" in summary.columns and "model_drift_score" not in summary.columns:
        summary["model_drift_score"] = pd.to_numeric(summary["prediction_drift_score"], errors="coerce")

    return summary, features


# ---------------------------------------------------------------------------
# Composed load function (for backward compat + overview tab)
# ---------------------------------------------------------------------------

def load_data(
    data_config: Dict[str, Any],
    model_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    model_version: str,
    refresh_nonce: int,
    repo_root: str,
) -> Dict[str, pd.DataFrame]:
    gold = get_feature_snapshot(
        data_config=data_config,
        model_config=model_config,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    predictions = get_predictions(
        data_config=data_config,
        model_config=model_config,
        symbol=symbol,
        start=start,
        end=end,
        model_version=model_version,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    drift_summary, drift_features = get_drift_history(
        model_config=model_config,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    return {
        "gold": gold,
        "predictions": predictions,
        "drift": drift_summary,
        "drift_features": drift_features,
    }


# ---------------------------------------------------------------------------
# PnL simulation
# ---------------------------------------------------------------------------

def simulate_pnl(
    predictions: pd.DataFrame,
    confidence_threshold: float = 0.0015,
) -> Dict[str, Any]:
    """
    Light backtest: long if pred > threshold, short if pred < -threshold.
    Returns dict with cumulative_return series, sharpe, hit_ratio.
    """
    if predictions.empty or "prediction" not in predictions.columns:
        return {"cum_return": pd.Series(dtype=float), "sharpe": None, "hit_ratio": None, "n_trades": 0}

    df = predictions[["open_time", "prediction", "actual_log_return_lead1"]].dropna().copy()
    df = df.sort_values("open_time")

    # Signals
    df["signal"] = 0
    df.loc[df["prediction"] > confidence_threshold, "signal"] = 1
    df.loc[df["prediction"] < -confidence_threshold, "signal"] = -1

    active = df[df["signal"] != 0].copy()
    if active.empty:
        return {"cum_return": pd.Series(dtype=float), "sharpe": None, "hit_ratio": None, "n_trades": 0}

    active["trade_return"] = active["signal"] * active["actual_log_return_lead1"]
    active["cum_return"] = active["trade_return"].cumsum()

    n = len(active)
    hits = (active["trade_return"] > 0).sum()
    hit_ratio = hits / n if n > 0 else None

    mean_r = active["trade_return"].mean()
    std_r = active["trade_return"].std()
    sharpe = (mean_r / std_r * np.sqrt(252 * 24)) if std_r and std_r > 0 else None

    return {
        "cum_return": active.set_index("open_time")["cum_return"],
        "trade_returns": active.set_index("open_time")["trade_return"],
        "sharpe": sharpe,
        "hit_ratio": hit_ratio,
        "n_trades": n,
    }


# ---------------------------------------------------------------------------
# Regime conditioning
# ---------------------------------------------------------------------------

def compute_regime_performance(
    predictions: pd.DataFrame,
    gold: pd.DataFrame,
    regime_col: str,
) -> pd.DataFrame:
    """Return accuracy metrics broken down by regime value."""
    if predictions.empty or gold.empty:
        return pd.DataFrame()

    regime_vals = None
    if regime_col in predictions.columns:
        regime_vals = predictions[[regime_col, "direction_correct", "abs_error", "open_time"]].copy()
    elif regime_col in gold.columns:
        regime_vals = gold[["open_time", regime_col]].merge(
            predictions[["open_time", "direction_correct", "abs_error"]],
            on="open_time",
            how="inner",
        )

    if regime_vals is None or regime_vals.empty:
        return pd.DataFrame()

    grouped = (
        regime_vals.groupby(regime_col)
        .agg(
            n=("direction_correct", "count"),
            dir_accuracy=("direction_correct", "mean"),
            mae=("abs_error", "mean"),
        )
        .reset_index()
    )
    return grouped


# ---------------------------------------------------------------------------
# Internal utilities (not cached – called from cached functions)
# ---------------------------------------------------------------------------

def _load_gold_minimal(
    data_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    refresh_nonce: int,
    repo_root: str,
) -> pd.DataFrame:
    gold_path = data_config.get("tables", {}).get("gold_market", {}).get("path", "")
    return load_delta_table(
        table_path=gold_path,
        columns=["open_time", "ingestion_time", "symbol", "close", "log_return", "date"],
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )


def compute_latest_updates(
    data_config: Dict[str, Any],
    symbol: str,
    start: datetime,
    end: datetime,
    refresh_nonce: int,
    repo_root: str,
) -> pd.DataFrame:
    gold_path = data_config.get("tables", {}).get("gold_market", {}).get("path", "")
    frame = load_delta_table(
        table_path=gold_path,
        columns=["open_time", "ingestion_time", "symbol", "date", "close", "log_return"],
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    open_time = frame["open_time"].max() if "open_time" in frame.columns and not frame.empty else pd.NaT
    ingest_time = frame["ingestion_time"].max() if "ingestion_time" in frame.columns and not frame.empty else pd.NaT
    latest_close = frame["close"].dropna().iloc[-1] if "close" in frame.columns and frame["close"].notna().any() else None
    latest_log_return = (
        frame["log_return"].dropna().iloc[-1]
        if "log_return" in frame.columns and frame["log_return"].notna().any()
        else None
    )

    return pd.DataFrame([{
        "Layer": "Gold",
        "Latest open_time": open_time,
        "Latest ingestion_time": ingest_time,
        "Latest close": latest_close,
        "Latest log_return": latest_log_return,
        "Rows in range": int(len(frame)),
    }])
