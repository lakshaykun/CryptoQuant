# models/features/feature_engineering.py

import pandas as pd
from utils_global.config_loader import load_config
from utils_global.logger import get_logger
import numpy as np

logger = get_logger(__name__)
model_config = load_config("configs/model.yaml")
data_config = load_config("configs/data.yaml")

def feature_engineering():
    """
    Perform feature engineering on the loaded dataset. This includes:
    - Encoding categorical variables (e.g., symbol) into integers.
    - Creating target variable 'log_return_lead1' which is the log return shifted by -1 (next time step's log return).
    - Saving the engineered dataset back to parquet for use in model training.
    """

    logger.info("Starting feature engineering...")

    data_path = model_config["train_data_path"]
    df = pd.read_parquet(data_path)

    # remove rows with missing values and with is_valid_feature = False
    df = df.dropna(subset=model_config.get("features", []))
    df = df[df["is_valid_feature_row"] == True].copy()

    df = df.sort_values(["symbol", "open_time"])
    df = df.reset_index(drop=True)

    # create target variable log_return_lead1
    df["next_close"] = df.groupby("symbol")["close"].shift(-1)
    df["log_return_lead1"] = np.log(df["next_close"] / df["close"])
    df["log_return_lead1"] = df["log_return_lead1"].clip(-0.01, 0.01)
    df = df.dropna(subset=["log_return_lead1"])

    # encoding symbol into integer and using all symbols from config
    symbol_map = {s: i for i, s in enumerate(data_config["symbols"])}
    df["symbol"] = df["symbol"].map(symbol_map).fillna(-1).astype(int)

    # filter to training window if specified in config
    if "train_window_days" in model_config:
        max_time = df["open_time"].max()
        min_time = max_time - pd.Timedelta(days=model_config["train_window_days"])
        df = df[df["open_time"] >= min_time]

    df = df.drop(columns=["next_close"])

    df.to_parquet(data_path)
    logger.info("Feature engineering completed and saved to parquet.")