import datetime
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pipelines.storage.delta.utils import get_table_config
from utils_global.config_loader import load_config
from utils_global.logger import get_logger

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
    last_value: Optional[datetime.datetime],
    symbols: Optional[List[str]] = None
) -> DataFrame:
    """
    Reads only new data after last_value.
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        if last_value is not None:
            df = df.filter(F.col("date") >= F.to_date(F.lit(last_value)))
            df = df.filter(F.col("open_time") > F.lit(last_value))

        if symbols is not None:
            df = df.filter(F.col("symbol").isin(symbols))

        logger.info(f"[{table_name}] Incremental read from {last_value}")

        return df

    except Exception as e:
        logger.error(f"[{table_name}] Incremental read failed → {e}")
        raise

def read_incremental_symbols(
    spark: SparkSession,
    table_name: str,
    last_values: dict
) -> DataFrame:
    """
    Reads only new data after last_value per symbol.
    """

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        meta_df = spark.createDataFrame([
            (symbol, ts) for symbol, ts in last_values.items()
        ], ["symbol", "last_time"])

        df = df.join(meta_df, "symbol") \
            .filter(F.col("open_time") > F.col("last_time"))

        logger.info(f"[{table_name}] Incremental read for symbols with last values")

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
# 🔹 Get Max Open Time (for pipelines)
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
    

def get_last_processed_time_symbols(
        spark: SparkSession,
        table_name: str,
        symbols: List[str],
        fallback_time: datetime.datetime
) -> dict:
    '''Returns last_processed_time per symbol from a state/checkpoint table.'''

    table_config = get_table_config(table_name, CONFIG)

    try:
        df = spark.read.format("delta").load(table_config["path"])

        df = df.filter(F.col("symbol").isin(symbols))

        result_df = (
            df.groupBy("symbol")
              .agg(F.max("last_processed_time").alias("last_processed_time"))
        )

        result = {
            symbol: fallback_time
            for symbol in symbols
        }

        for row in result_df.collect():
            if row["last_processed_time"] is not None:
                result[row["symbol"]] = row["last_processed_time"]

        return result

    except Exception as e:
        logger.warning(f"[{table_name}] No state data found or error → {e}")
        return {symbol: fallback_time for symbol in symbols}
    

def check_table_exists(spark: SparkSession, table_name: str) -> bool:
    '''Checks if Delta table exists.'''

    table_config = get_table_config(table_name, CONFIG)

    try:
        spark.read.format("delta").load(table_config["path"]).limit(1).collect()
        return True
    except Exception as e:
        logger.warning(f"Table {table_name} does not exist or is empty: {e}")
        return False