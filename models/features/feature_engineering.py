# models/features/feature_engineering.py

import pandas as pd
from utils_global.config_loader import load_config
from utils_global.logger import get_logger

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
    df = df.dropna()
    df = df[df["is_valid_feature_row"] == True]

    df = df.reset_index(drop=True)

    # encoding symbol into integer and using all symbols from config
    symbols = data_config["symbols"]
    df = df.copy()
    df["symbol"] = df["symbol"].apply(lambda x: symbols.index(x) if x in symbols else -1)

    # create target variable
    df["log_return_lead1"] = df["close"].shift(-1) / df["close"]

    df.to_parquet(data_path)
    logger.info("Feature engineering completed and saved to parquet.")