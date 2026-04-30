import datetime
import pandas as pd
from deltalake import DeltaTable
from data_platform.utils_global.logger import get_logger
from data_platform.utils_global.config_loader import load_config

logger = get_logger(__name__)
CONFIG = load_config("data_platform/configs/data.yaml")

GOLD_COLUMNS  = ["window_start", "symbol", "sentiment_index", "avg_confidence", "message_count", "window_date"]
SILVER_COLUMNS = ["event_time", "symbol", "source", "engagement"]

def get_gold_path() -> str:
    return CONFIG["tables"]["gold_sentiment"]["path"]

def get_silver_path() -> str:
    return CONFIG["tables"]["silver_sentiment"]["path"]

def read_gold_full(symbols: list[str] | None = None) -> pd.DataFrame:
    try:
        df = DeltaTable(get_gold_path()).to_pandas()
        df = df[GOLD_COLUMNS]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[sentiment/reader] Gold full read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[sentiment/reader] Gold full read failed → {e}")
        raise


def read_gold_incremental(
    last_synced_map: dict,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    try:
        df = DeltaTable(get_gold_path()).to_pandas()
        df = df[GOLD_COLUMNS]
        # Ensure last_synced_time is timezone-naive before localizing, or convert if already tz-aware
        if last_synced_time.tzinfo is not None:
            ts = pd.Timestamp(last_synced_time).tz_convert("UTC")
        else:
            ts = pd.Timestamp(last_synced_time).tz_localize("UTC")
        df = df[df["window_start"] > ts]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[sentiment/reader] Gold incremental read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[sentiment/reader] Gold incremental read failed → {e}")
        raise


def read_silver_full(symbols: list[str] | None = None) -> pd.DataFrame:
    try:
        df = DeltaTable(get_silver_path()).to_pandas()
        df = df[SILVER_COLUMNS]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[sentiment/reader] Silver full read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[sentiment/reader] Silver full read failed → {e}")
        raise


def read_silver_incremental(
    last_synced_map: dict,
    symbols: list[str] | None = None
) -> pd.DataFrame:
    try:
        df = DeltaTable(get_silver_path()).to_pandas()
        df = df[SILVER_COLUMNS]
        # Ensure last_synced_time is timezone-naive before localizing, or convert if already tz-aware
        if last_synced_time.tzinfo is not None:
            ts = pd.Timestamp(last_synced_time).tz_convert("UTC")
        else:
            ts = pd.Timestamp(last_synced_time).tz_localize("UTC")
        df = df[df["event_time"] > ts]
        if symbols:
            df = df[df["symbol"].isin(symbols)]
        logger.info(f"[sentiment/reader] Silver incremental read → {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"[sentiment/reader] Silver incremental read failed → {e}")
        raise