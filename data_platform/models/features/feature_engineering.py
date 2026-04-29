# models/features/feature_engineering.py

import pandas as pd
from utils_global.config_loader import load_config
from utils_global.logger import get_logger
import numpy as np
from models.utils.horizon import resolve_horizon_steps

logger = get_logger(__name__)
model_config = load_config("configs/model.yaml")
data_config = load_config("configs/data.yaml")

def _compute_sign(value: float, threshold: float) -> int:
    if value > threshold:
        return 1
    if value < -threshold:
        return -1
    return 0


def feature_engineering():
    """Build model-ready dataset with dynamic multi-target labels."""

    logger.info("Starting feature engineering...")

    data_path = model_config["train_data_path"]
    df = pd.read_parquet(data_path)

    # remove rows with missing values and with is_valid_feature = False
    df = df.dropna(subset=model_config.get("features", []))
    df = df[df["is_valid_feature_row"] == True].copy()

    df = df.sort_values(["symbol", "open_time"])
    df = df.reset_index(drop=True)

    interval = data_config.get("interval", "5m")
    models_cfg = model_config.get("models", {})
    sign_cfg = model_config.get("targets", {}).get("sign", {})
    threshold = float(sign_cfg.get("threshold", 0.0002))
    drop_neutral = bool(sign_cfg.get("drop_neutral", False))

    # Preserve backward compatibility with legacy training configs.
    if not models_cfg:
        models_cfg = {
            "return_short": {"horizon": "short", "type": "regression"},
            "sign_short": {"horizon": "short", "type": "classification"},
        }

    close_by_symbol = df.groupby("symbol")["close"]

    for target_name, target_cfg in models_cfg.items():
        horizon = target_cfg.get("horizon", "short")
        target_type = target_cfg.get("type", "regression")
        steps = resolve_horizon_steps(horizon, interval)
        future_close = close_by_symbol.shift(-steps)
        return_series = np.log(future_close / df["close"])

        if target_type == "classification":
            df[target_name] = return_series.apply(lambda value: _compute_sign(value, threshold))
        else:
            df[target_name] = return_series

        if target_name == "return_short":
            df["log_return_lead1"] = return_series

    # Ensure required canonical targets exist for downstream compatibility.
    if "return_short" not in df.columns:
        future_close = close_by_symbol.shift(-1)
        df["return_short"] = np.log(future_close / df["close"])
    if "return_long" not in df.columns:
        df["return_long"] = df["return_short"]
    if "sign_short" not in df.columns:
        df["sign_short"] = df["return_short"].apply(lambda value: _compute_sign(value, threshold))
    if "sign_long" not in df.columns:
        df["sign_long"] = df["return_long"].apply(lambda value: _compute_sign(value, threshold))
    if "log_return_lead1" not in df.columns:
        df["log_return_lead1"] = df["return_short"]

    df = df.dropna(subset=["return_short", "return_long"])
    if drop_neutral:
        df = df[np.abs(df["return_short"]) > threshold]

    # encoding symbol into integer and using all symbols from config
    symbol_map = {s: i for i, s in enumerate(data_config["symbols"])}
    df["symbol"] = df["symbol"].map(symbol_map).fillna(-1).astype(int)

    # filter to training window if specified in config
    if "train_window_days" in model_config:
        max_time = df["open_time"].max()
        min_time = max_time - pd.Timedelta(days=model_config["train_window_days"])
        df = df[df["open_time"] >= min_time]

    df.to_parquet(data_path)
    logger.info("Feature engineering completed and saved to parquet.")