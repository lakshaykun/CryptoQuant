from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from delta_client import load_delta_table
from helpers import to_utc_datetime
import numpy as np

def prepare_predictions_frame(
    frame: pd.DataFrame,
    gold_frame: pd.DataFrame,
    symbol: str,
    model_version: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame

    subset = frame.copy()
    subset = subset[subset["symbol"] == symbol]

    if "model_version" in subset.columns and model_version != "All":
        subset = subset[subset["model_version"].astype(str) == model_version]

    subset = subset.sort_values(["symbol", "open_time"])

    target_frame = gold_frame[["symbol", "open_time", "close", "log_return"]].copy()
    target_frame = target_frame.sort_values(["symbol", "open_time"])
    target_frame["actual_close"] = target_frame.groupby("symbol")["close"].shift(-1)
    target_frame["actual_log_return_lead1"] = target_frame.groupby("symbol")["log_return"].shift(-1)
    target_frame = target_frame[["symbol", "open_time", "actual_close", "actual_log_return_lead1"]]

    subset = subset.merge(target_frame, on=["symbol", "open_time"], how="left")
    subset["predicted_close"] = subset["close"] * np.exp(subset["prediction"])
    subset["close_residual"] = subset["predicted_close"] - subset["actual_close"]
    subset["residual"] = subset["prediction"] - subset["actual_log_return_lead1"]

    return subset.dropna(subset=["prediction", "actual_log_return_lead1", "predicted_close", "actual_close"]).copy()


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
    gold_path = data_config.get("tables", {}).get("gold_market", {}).get("path", "")
    feature_columns = [feature for feature in model_config.get("features", []) if feature != "symbol"]
    gold_columns = list(
        dict.fromkeys(["open_time", "ingestion_time", "symbol", "close", "log_return", "date", *feature_columns])
    )
    gold_frame = load_delta_table(
        table_path=gold_path,
        columns=gold_columns,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    predictions_path = data_config.get("tables", {}).get("predictions_log_return_lead1", {}).get("path", "")
    prediction_columns = ["open_time", "symbol", "close", "prediction", "log_return", "model_version"]
    predictions_frame = load_delta_table(
        table_path=predictions_path,
        columns=prediction_columns,
        symbol=symbol,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )
    predictions_frame = prepare_predictions_frame(
        predictions_frame,
        gold_frame=gold_frame,
        symbol=symbol,
        model_version=model_version,
    )

    drift_path = model_config.get("monitoring", {}).get("history", {}).get("path", "")
    drift_columns = [
        "event_time",
        "drift_score",
        "data_drift_score",
        "prediction_drift_score",
        "drift_detected",
        "triggered",
        "trigger_reason",
    ]
    drift_frame = load_delta_table(
        table_path=drift_path,
        columns=drift_columns,
        start=start,
        end=end,
        refresh_nonce=refresh_nonce,
        repo_root=repo_root,
    )

    if "event_time" in drift_frame.columns:
        drift_frame["event_time"] = to_utc_datetime(drift_frame["event_time"])

    return {
        "gold": gold_frame,
        "predictions": predictions_frame,
        "drift": drift_frame,
    }


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

    return pd.DataFrame(
        [
            {
                "Layer": "Gold",
                "Latest open_time": open_time,
                "Latest ingestion_time": ingest_time,
                "Latest close": latest_close,
                "Latest log_return": latest_log_return,
                "Rows in range": int(len(frame)),
            }
        ]
    )
