from pipelines.jobs.streaming.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

import csv
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from pipelines.jobs.streaming.common.job_runtime import await_query, configure_job_logging
from pipelines.transformers.gold.sentiment import GoldSentimentTransformer
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_gold_trigger_seconds,
    get_gold_watermark_seconds,
    get_gold_window_seconds,
    get_spark_app_name,
    get_spark_master,
)
from utils.spark_utils import create_delta_spark_session

logger = logging.getLogger(__name__)

SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
GOLD_DELTA_PATH = get_delta_path("gold", "delta/gold")
# Use a dedicated checkpoint namespace for the realtime gold+csv sink.
GOLD_CHECKPOINT_PATH = f"{get_checkpoint_path('gold', 'checkpoints/gold')}_realtime_csv_v1"
GOLD_WINDOW_SECONDS = get_gold_window_seconds(60)
GOLD_WINDOW_DURATION = f"{GOLD_WINDOW_SECONDS} seconds"
GOLD_WATERMARK_SECONDS = get_gold_watermark_seconds(600)
GOLD_WATERMARK_DURATION = f"{GOLD_WATERMARK_SECONDS} seconds"
GOLD_TRIGGER_SECONDS = get_gold_trigger_seconds(5)
APP_NAME = f"{get_spark_app_name()}-gold"
GOLD_REALTIME_CSV_PATH = str((Path(GOLD_DELTA_PATH).parent / "gold_sentiment_realtime.csv").resolve())
EMA_LOOKBACK = 10
CSV_HEADERS = [
    "minute_time",
    "symbol",
    "sentiment_index",
    "total_engagement",
    "avg_confidence",
    "message_count",
    "is_observed",
]


@dataclass
class CsvSymbolState:
    last_minute: datetime | None = None
    sentiment_history: deque[float] = field(default_factory=lambda: deque(maxlen=EMA_LOOKBACK))
    last_avg_confidence: float = 0.0


_csv_state_by_symbol: dict[str, CsvSymbolState] = {}
_csv_state_initialized = False

silver_schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("label", StringType()),
    StructField("confidence", DoubleType()),
    StructField("weighted_sentiment", DoubleType()),
])


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def build_silver_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    ensure_delta_table(spark, SILVER_DELTA_PATH, silver_schema)
    return (
        spark.readStream
        .format("delta")
        .option("startingVersion", "0")
        .load(SILVER_DELTA_PATH)
    )


def build_gold_aggregation(silver_stream):
    return GoldSentimentTransformer.transform(
        silver_stream,
        window_duration=GOLD_WINDOW_DURATION,
        watermark_duration=GOLD_WATERMARK_DURATION,
    )


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ema_from_recent(values: list[float], window: int = EMA_LOOKBACK) -> float:
    span = max(1, window)
    alpha = 2.0 / (span + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (alpha * value) + ((1.0 - alpha) * ema)
    return float(ema)


def _minute_range(start_minute: datetime, end_minute: datetime):
    minute = start_minute
    while minute <= end_minute:
        yield minute
        minute += timedelta(minutes=1)


def _ensure_realtime_csv() -> Path:
    csv_path = Path(GOLD_REALTIME_CSV_PATH)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writeheader()
    return csv_path


def _load_csv_state_from_disk(csv_path: Path) -> None:
    if not csv_path.exists():
        return

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = (row.get("symbol") or "").strip()
            minute_text = (row.get("minute_time") or "").strip()
            sentiment_text = (row.get("sentiment_index") or "").strip()
            if not symbol or not minute_text or not sentiment_text:
                continue

            try:
                minute_time = datetime.fromisoformat(minute_text)
                sentiment_value = float(sentiment_text)
            except ValueError:
                continue

            state = _csv_state_by_symbol.setdefault(symbol, CsvSymbolState())
            state.sentiment_history.append(sentiment_value)
            if state.last_minute is None or minute_time >= state.last_minute:
                state.last_minute = minute_time
                state.last_avg_confidence = _to_float(
                    row.get("avg_confidence"),
                    state.last_avg_confidence,
                )


def _initialize_csv_state() -> Path:
    global _csv_state_initialized

    csv_path = _ensure_realtime_csv()
    if not _csv_state_initialized:
        _load_csv_state_from_disk(csv_path)
        _csv_state_initialized = True
    return csv_path


def _extract_actual_rows_by_minute(batch_df) -> dict[str, dict[datetime, dict[str, float | int]]]:
    minute_df = (
        batch_df
        .select(
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("sentiment_index").cast("double").alias("sentiment_index"),
            F.col("total_engagement").cast("double").alias("total_engagement"),
            F.col("avg_confidence").cast("double").alias("avg_confidence"),
            F.col("message_count").cast("long").alias("message_count"),
        )
        .where(F.col("window_end").isNotNull() & F.col("symbol").isNotNull())
        .withColumn("minute_time", F.date_trunc("minute", F.col("window_end")))
        .groupBy("minute_time", "symbol")
        .agg(
            F.avg("sentiment_index").alias("sentiment_index"),
            F.sum("total_engagement").alias("total_engagement"),
            F.avg("avg_confidence").alias("avg_confidence"),
            F.sum("message_count").alias("message_count"),
        )
        .orderBy("minute_time", "symbol")
    )

    extracted: dict[str, dict[datetime, dict[str, float | int]]] = defaultdict(dict)
    for row in minute_df.collect():
        symbol = row["symbol"]
        minute_time = row["minute_time"]
        if symbol is None or minute_time is None:
            continue

        extracted[symbol][minute_time] = {
            "sentiment_index": _to_float(row["sentiment_index"], 0.0),
            "total_engagement": _to_float(row["total_engagement"], 0.0),
            "avg_confidence": _to_float(row["avg_confidence"], 0.0),
            "message_count": _to_int(row["message_count"], 0),
        }

    return dict(extracted)


def _build_realtime_csv_rows(
    actual_rows_by_symbol: dict[str, dict[datetime, dict[str, float | int]]],
) -> list[dict[str, str]]:
    rows_to_write: list[dict[str, str]] = []
    now_minute = datetime.now().replace(second=0, microsecond=0)

    symbols = set(_csv_state_by_symbol.keys()) | set(actual_rows_by_symbol.keys())
    for symbol in sorted(symbols):
        state = _csv_state_by_symbol.setdefault(symbol, CsvSymbolState())
        actual_by_minute = actual_rows_by_symbol.get(symbol, {})
        latest_actual_minute = max(actual_by_minute.keys()) if actual_by_minute else None

        # Emit at most one row per symbol per wall-clock minute.
        # This avoids large catch-up backfills when the stream is idle.
        if state.last_minute is not None and state.last_minute >= now_minute:
            continue

        minute_time = now_minute
        observed = actual_by_minute.get(minute_time)

        if observed is None and state.last_minute is None and latest_actual_minute is not None:
            minute_time = latest_actual_minute
            observed = actual_by_minute.get(minute_time)

        if observed is not None:
            sentiment_index = _to_float(observed["sentiment_index"], 0.0)
            total_engagement = _to_float(observed["total_engagement"], 0.0)
            avg_confidence = _to_float(observed["avg_confidence"], state.last_avg_confidence)
            message_count = _to_int(observed["message_count"], 0)
            is_observed = True
        else:
            if not state.sentiment_history:
                continue
            sentiment_index = _ema_from_recent(list(state.sentiment_history), EMA_LOOKBACK)
            total_engagement = 0.0
            avg_confidence = state.last_avg_confidence
            message_count = 0
            is_observed = False

        rows_to_write.append(
            {
                "minute_time": minute_time.isoformat(),
                "symbol": symbol,
                "sentiment_index": f"{sentiment_index:.6f}",
                "total_engagement": f"{total_engagement:.6f}",
                "avg_confidence": f"{avg_confidence:.6f}",
                "message_count": str(message_count),
                "is_observed": "true" if is_observed else "false",
            }
        )

        state.sentiment_history.append(sentiment_index)
        state.last_avg_confidence = avg_confidence
        state.last_minute = minute_time

    return rows_to_write


def _append_rows_to_csv(csv_path: Path, rows_to_write: list[dict[str, str]]) -> None:
    if not rows_to_write:
        return

    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writerows(rows_to_write)


def _upsert_gold_delta(df) -> None:
    if df is None or df.rdd.isEmpty():
        return

    if not DeltaTable.isDeltaTable(df.sparkSession, GOLD_DELTA_PATH):
        df.write.format("delta").mode("append").save(GOLD_DELTA_PATH)
        return

    target = DeltaTable.forPath(df.sparkSession, GOLD_DELTA_PATH)
    (
        target.alias("t")
        .merge(
            df.alias("s"),
            "t.symbol = s.symbol AND t.window.start = s.window.start AND t.window.end = s.window.end",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _build_fallback_delta_df(batch_df, rows_to_write: list[dict[str, str]]):
    fallback_rows: list[Row] = []
    for row in rows_to_write:
        if row.get("is_observed") != "false":
            continue

        try:
            minute_start = datetime.fromisoformat(str(row["minute_time"]))
        except (TypeError, ValueError, KeyError):
            continue

        fallback_rows.append(
            Row(
                window=Row(
                    start=minute_start,
                    end=minute_start + timedelta(minutes=1),
                ),
                symbol=str(row.get("symbol", "")).strip(),
                sentiment_index=_to_float(row.get("sentiment_index"), 0.0),
                total_engagement=_to_float(row.get("total_engagement"), 0.0),
                avg_confidence=_to_float(row.get("avg_confidence"), 0.0),
                message_count=_to_int(row.get("message_count"), 0),
            )
        )

    if not fallback_rows:
        return None

    # Build from local rows and cast to the streaming schema to avoid python-side
    # type mismatches (for example float values for LongType fields).
    fallback_df = batch_df.sparkSession.createDataFrame(fallback_rows)
    cast_columns = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in batch_df.schema.fields
    ]
    return fallback_df.select(*cast_columns)


def process_gold_batch(batch_df, epoch_id: int) -> None:
    csv_path = _initialize_csv_state()

    has_batch_data = not batch_df.rdd.isEmpty()
    actual_rows_by_symbol: dict[str, dict[datetime, dict[str, float | int]]] = {}
    observed_groups = 0

    if has_batch_data:
        batch_df.persist()
        try:
            _upsert_gold_delta(batch_df)
            actual_rows_by_symbol = _extract_actual_rows_by_minute(batch_df)
            observed_groups = sum(len(rows) for rows in actual_rows_by_symbol.values())
        finally:
            batch_df.unpersist()

    rows_to_write = _build_realtime_csv_rows(actual_rows_by_symbol)
    _append_rows_to_csv(csv_path, rows_to_write)

    fallback_delta_df = _build_fallback_delta_df(batch_df, rows_to_write)
    _upsert_gold_delta(fallback_delta_df)
    fallback_groups = sum(1 for row in rows_to_write if row.get("is_observed") == "false")

    logger.info(
        "Gold epoch=%s input_rows=%d observed_groups=%d fallback_groups=%d csv_rows=%d csv_path=%s",
        epoch_id,
        1 if has_batch_data else 0,
        observed_groups,
        fallback_groups,
        len(rows_to_write),
        csv_path,
    )


def run() -> None:
    configure_job_logging()
    silver_stream = build_silver_stream()
    gold_stream = build_gold_aggregation(silver_stream)
    logger.info("Real-time gold CSV output: %s", GOLD_REALTIME_CSV_PATH)

    query: StreamingQuery = (
        gold_stream.writeStream
        .queryName(f"{APP_NAME}-write")
        .outputMode("update")
        .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
        .trigger(processingTime=f"{GOLD_TRIGGER_SECONDS} seconds")
        .foreachBatch(process_gold_batch)
        .start()
    )
    await_query(query)


if __name__ == "__main__":
    run()
