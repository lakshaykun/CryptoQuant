import datetime
import pandas as pd
from deltalake import DeltaTable
from data_platform.utils_global.logger import get_logger
from data_platform.utils_global.config_loader import load_config

logger = get_logger(__name__)

CONFIG = load_config("data_platform/configs/data.yaml")


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

        if symbols:
            df = df[df["symbol"].isin(symbols)]

        logger.info(f"[reader] Full read → {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"[reader] Full read failed → {e}")
        raise


def read_incremental(
    last_synced_time: datetime.datetime,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    """
    Reads only rows after last_synced_time. Used for periodic sync.
    """
    try:
        table = DeltaTable(get_gold_path())
        df = table.to_pandas()

        df = df[df["open_time"] > pd.Timestamp(last_synced_time, tz="UTC")]

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
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[reader] Predictions full read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[reader] Predictions full read failed → {e}")
        raise


def read_predictions_incremental(
    last_synced_time: datetime.datetime,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    try:
        table = DeltaTable(get_predictions_path())
        df = table.to_pandas()
        df = df[df["open_time"] > pd.Timestamp(last_synced_time, tz="UTC")]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[reader] Predictions incremental read from {last_synced_time} → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[reader] Predictions incremental read failed → {e}")
        raise
