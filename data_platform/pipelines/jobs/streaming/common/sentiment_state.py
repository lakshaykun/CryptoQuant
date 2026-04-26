from __future__ import annotations

import random
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pipelines.schema.state.sentiment import STATE_SENTIMENT_SCHEMA
from utils.config_loader import load_config


def get_sentiment_state_path(default_relative: str = "delta/state/sentiment") -> str:
    config = load_config("configs/data.yaml") or {}
    tables = config.get("tables", {}) if isinstance(config, dict) else {}
    state_config = tables.get("sentiment_state", {}) if isinstance(tables, dict) else {}
    configured_path = ""
    if isinstance(state_config, dict):
        configured_path = str(state_config.get("path", "")).strip()
    if configured_path:
        return configured_path
    return str((Path(__file__).resolve().parents[4] / default_relative).resolve())


def ensure_sentiment_state_table(
    spark: SparkSession,
    state_path: str,
    schema: StructType = STATE_SENTIMENT_SCHEMA,
) -> None:
    if DeltaTable.isDeltaTable(spark, state_path):
        return

    (
        spark.createDataFrame([], schema)
        .write
        .format("delta")
        .mode("ignore")
        .partitionBy("layer", "source", "symbol", "state_date")
        .save(state_path)
    )


def build_state_updates(events_df: DataFrame, layer: str) -> DataFrame:
    return (
        events_df
        .where(F.col("event_time").isNotNull())
        .groupBy("source", "symbol")
        .agg(F.max("event_time").alias("last_processed_time"))
        .withColumn("layer", F.lit(layer))
        .withColumn("state_date", F.to_date("last_processed_time"))
        .withColumn("ingestion_time", F.current_timestamp())
        .select(
            "layer",
            "source",
            "symbol",
            "last_processed_time",
            "state_date",
            "ingestion_time",
        )
    )


def upsert_sentiment_state(events_df: DataFrame, layer: str, state_path: str | None = None) -> None:
    if events_df is None or events_df.limit(1).count() == 0:
        return

    spark = events_df.sparkSession
    target_state_path = state_path or get_sentiment_state_path()
    updates = build_state_updates(events_df, layer)
    if updates.limit(1).count() == 0:
        return

    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            ensure_sentiment_state_table(spark, target_state_path)
            target = DeltaTable.forPath(spark, target_state_path)
            (
                target.alias("t")
                .merge(
                    updates.alias("s"),
                    "t.layer = s.layer AND t.source = s.source AND t.symbol = s.symbol",
                )
                .whenMatchedUpdate(
                    set={
                        "last_processed_time": "s.last_processed_time",
                        "state_date": "s.state_date",
                        "ingestion_time": "s.ingestion_time",
                    }
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt == 5:
                break
            # Parallel source ingest tasks can conflict on optimistic Delta commits.
            time.sleep((0.25 * attempt) + random.uniform(0.0, 0.2))

    if last_error is not None:
        raise last_error
