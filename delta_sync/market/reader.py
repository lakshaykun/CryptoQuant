import datetime
import pandas as pd
from deltalake import DeltaTable
from data_platform.utils_global.logger import get_logger
from data_platform.utils_global.config_loader import load_config

logger = get_logger(__name__)

CONFIG = load_config("data_platform/configs/data.yaml")

MARKET_COLUMNS = [
    "open_time", "symbol", "open", "high", "low", "close", "volume", "trades",
    "taker_buy_base", "log_return", "volatility", "imbalance_ratio", "buy_ratio",
    "log_return_lag1", "log_return_lag2", "buy_ratio_lag1", "ma_5", "ma_20",
    "volatility_5", "volume_5", "buy_ratio_5", "momentum", "volume_spike",
    "price_range_ratio", "body_size", "hour", "day_of_week", "trend_strength",
    "volatility_ratio", "is_valid_feature_row", "date", "ingestion_time"
]

PREDICTIONS_COLUMNS = [
    "open_time", "symbol", "date", "prediction", "ingestion_time",
    "predicted_close", "actual_close", "error", "direction_correct"
]


def get_gold_path() -> str:
    return CONFIG["tables"]["gold_market"]["path"]

def get_predictions_path() -> str:
    return CONFIG["tables"]["predictions_log_return_lead1"]["path"]

def read_full(symbols: list[str] | None = None) -> pd.DataFrame:
    """
    Reads the entire gold table. Used for initial load.
    """
    try:
        table = DeltaTable(get_gold_path())
        df = table.to_pandas()
        df = df[[c for c in MARKET_COLUMNS if c in df.columns]]

        if symbols:
            df = df[df["symbol"].isin(symbols)]

        logger.info(f"[reader] Full read → {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"[reader] Full read failed → {e}")
        raise


def read_incremental(
    last_synced_map: dict,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    """
    Reads only rows after last_synced_time. Used for periodic sync.
    """
    try:
        table = DeltaTable(get_gold_path())
        df = table.to_pandas()
        df = df[[c for c in MARKET_COLUMNS if c in df.columns]]

        last_df = pd.DataFrame(list(last_synced_map.items()), columns=['symbol', 'last_sync'])
        last_df['last_sync'] = pd.to_datetime(last_df['last_sync'], utc=True)
        df = df.merge(last_df, on='symbol', how='left')
        df = df[df['last_sync'].isna() | (df['open_time'] > df['last_sync'])]
        df = df.drop(columns=['last_sync'])

        if symbols:
            df = df[df["symbol"].isin(symbols)]

        logger.info(f"[reader] Incremental read from {last_synced_time} → {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"[reader] Incremental read failed → {e}")
        raise


def read_predictions_full(symbols: list[str] | None = None) -> pd.DataFrame:
    try:
        table = DeltaTable(get_predictions_path())
        df = table.to_pandas()
        df = df[[c for c in PREDICTIONS_COLUMNS if c in df.columns]]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[reader] Predictions full read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[reader] Predictions full read failed → {e}")
        raise


def read_predictions_incremental(
    last_synced_map: dict,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    try:
        table = DeltaTable(get_predictions_path())
        df = table.to_pandas()
        df = df[[c for c in PREDICTIONS_COLUMNS if c in df.columns]]
        last_df = pd.DataFrame(list(last_synced_map.items()), columns=['symbol', 'last_sync'])
        last_df['last_sync'] = pd.to_datetime(last_df['last_sync'], utc=True)
        df = df.merge(last_df, on='symbol', how='left')
        df = df[df['last_sync'].isna() | (df['open_time'] > df['last_sync'])]
        df = df.drop(columns=['last_sync'])
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[reader] Predictions incremental read from {last_synced_time} → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[reader] Predictions incremental read failed → {e}")
        raise
