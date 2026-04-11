import datetime
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pipelines.storage.delta.utils import get_table_config
from pipelines.utils.config_loader import load_config
from pipelines.utils.logger import get_logger

logger = get_logger(__name__)

CONFIG = load_config("configs/data.yaml")


# ---------------------------
# 🔹 Base Read
# ---------------------------

def read_table(
    spark: SparkSession,
    table_name: str,
    columns: Optional[List[str]] = None,
    filters: Optional[str] = None
) -> DataFrame:
    """
    Reads full table (optionally filtered).
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        if columns:
            df = df.select(*columns)

        if filters:
            df = df.filter(filters)

        logger.info(f"[{table_name}] Read successful")

        return df

    except Exception as e:
        logger.error(f"[{table_name}] Read failed → {e}")
        raise


# ---------------------------
# 🔹 Incremental Read
# ---------------------------

def read_incremental(
    spark: SparkSession,
    table_name: str,
    timestamp_col: str,
    last_value: Optional[datetime.datetime]
) -> DataFrame:
    """
    Reads only new data after last_value.
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        if last_value is not None:
            df = df.filter(F.col(timestamp_col) > F.lit(last_value))

        logger.info(f"[{table_name}] Incremental read from {last_value}")

        return df

    except Exception as e:
        logger.error(f"[{table_name}] Incremental read failed → {e}")
        raise


# ---------------------------
# 🔹 Time Travel (Delta Feature)
# ---------------------------

def read_version(
    spark: SparkSession,
    table_name: str,
    version: int
) -> DataFrame:
    """
    Reads specific version of Delta table.
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = (
            spark.read
            .format("delta")
            .option("versionAsOf", version)
            .load(table_config["path"])
        )

        logger.info(f"[{table_name}] Read version {version}")

        return df

    except Exception as e:
        logger.error(f"[{table_name}] Version read failed → {e}")
        raise


# ---------------------------
# 🔹 Read Latest Snapshot by Date
# ---------------------------

def read_latest_partition(
    spark: SparkSession,
    table_name: str,
    partition_col: str = "date"
) -> DataFrame:
    """
    Reads only latest partition (fast query).
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        latest_value = df.select(F.max(partition_col)).collect()[0][0]

        df = df.filter(F.col(partition_col) == latest_value)

        logger.info(f"[{table_name}] Read latest partition → {latest_value}")

        return df

    except Exception as e:
        logger.error(f"[{table_name}] Latest partition read failed → {e}")
        raise


# ---------------------------
# 🔹 Get Max Timestamp (for pipelines)
# ---------------------------

def get_last_value(
    spark: SparkSession,
    table_name: str,
    column: str
):
    """
    Returns max value of a column (used for incremental pipelines).
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        result = df.select(F.max(column)).collect()[0][0]

        logger.info(f"[{table_name}] Last value for {column} → {result}")

        return result

    except Exception as e:
        logger.warning(f"[{table_name}] No data found or error → {e}")
        return None
    

def get_last_timestamp_symbols(
        spark: SparkSession, 
        table_name: str,
        symbols: List[str], 
        start_date: datetime.datetime
) -> dict:
    '''Returns max timestamp per symbol (used for incremental pipelines).'''

    table_config = get_table_config(table_name, CONFIG)
    
    try:
        df = spark.read.format("delta").load(table_config["path"])

        # Filter only required symbols (important for performance)
        df = df.filter(F.col("symbol").isin(symbols))

        # Get max timestamp per symbol
        result_df = (
            df.groupBy("symbol")
              .agg(F.max("open_time").alias("max_time"))
        )

        # Convert to dict: symbol -> timestamp
        result = {
            row["symbol"]: row["max_time"]
            for row in result_df.collect()
            if row["max_time"] is not None
        }

        # Ensure all symbols exist in output (even if no data)
        for symbol in symbols:
            if symbol not in result:
                result[symbol] = start_date

        return result

    except Exception as e:
        logger.warning(f"No existing bronze data found: {e}")
        return {symbol: start_date for symbol in symbols}