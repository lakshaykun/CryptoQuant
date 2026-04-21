from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

import argparse
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.spark_config import get_delta_path, get_spark_app_name, get_spark_master
from utils.spark_utils import create_delta_spark_session

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
GOLD_DELTA_PATH = get_delta_path("gold", "delta/gold")
APP_NAME = f"{get_spark_app_name()}-ingestion-metrics"


def _table_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _minute_counts(df: DataFrame, minute_expr, cutoff_ts: datetime) -> DataFrame:
    return (
        df
        .withColumn("minute", F.date_trunc("minute", minute_expr))
        .where(F.col("minute").isNotNull() & (F.col("minute") >= F.lit(cutoff_ts)))
        .groupBy("minute")
        .count()
        .orderBy(F.col("minute").desc())
    )


def _report_for_layer(spark, layer: str, path: str, minute_expr, cutoff_ts: datetime) -> None:
    print(f"\n=== {layer.upper()} ({path}) ===")

    if not _table_exists(spark, path):
        print("Delta table not found.")
        return

    source_df = spark.read.format("delta").load(path)
    result_df = _minute_counts(source_df, minute_expr, cutoff_ts)

    rows = result_df.collect()
    if not rows:
        print("No rows found in selected lookback window.")
        return

    print("minute,row_count")
    for row in rows:
        print(f"{row['minute']},{row['count']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show per-minute ingested row counts for Bronze/Silver/Gold Delta tables.",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=60,
        help="Lookback window in minutes (default: 60)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    lookback_minutes = max(1, int(args.minutes))
    cutoff_ts = datetime.now() - timedelta(minutes=lookback_minutes)

    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())

    try:
        _report_for_layer(
            spark,
            "bronze",
            BRONZE_DELTA_PATH,
            F.col("kafka_timestamp"),
            cutoff_ts,
        )
        _report_for_layer(
            spark,
            "silver",
            SILVER_DELTA_PATH,
            F.col("event_time"),
            cutoff_ts,
        )
        _report_for_layer(
            spark,
            "gold",
            GOLD_DELTA_PATH,
            F.col("window.end"),
            cutoff_ts,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
