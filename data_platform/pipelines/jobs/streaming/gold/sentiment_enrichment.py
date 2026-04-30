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
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from pipelines.jobs.streaming.common.job_runtime import await_query, configure_job_logging
from pipelines.jobs.streaming.common.sentiment_client import CryptoBertClient
from pipelines.jobs.streaming.common.sentiment_scoring import score_sentiment_dataframe
from pipelines.jobs.streaming.common.sentiment_state import upsert_sentiment_state
from pipelines.storage.delta.utils import get_table_config
from pipelines.transformers.gold.sentiment import GoldSentimentTransformer
from utils.config_loader import load_config
from utils.spark_config import (
    get_checkpoint_path,
    get_gold_sentiment_chunk_size,
    get_gold_sentiment_endpoint,
    get_gold_sentiment_max_concurrent_requests,
    get_gold_sentiment_timeout_seconds,
    get_gold_trigger_seconds,
    get_gold_watermark_seconds,
    get_gold_window_seconds,
    get_spark_app_name,
    get_spark_master,
)
from utils.spark_utils import create_delta_spark_session

logger = logging.getLogger(__name__)

DATA_CONFIG = load_config("configs/data.yaml")
SILVER_DELTA_PATH = get_table_config("silver_sentiment", DATA_CONFIG)["path"]
GOLD_DELTA_PATH = get_table_config("gold_sentiment", DATA_CONFIG)["path"]
# Use a dedicated checkpoint namespace for the realtime gold+csv sink.
GOLD_CHECKPOINT_PATH = f"{get_checkpoint_path('gold', 'checkpoints/gold')}_realtime_csv_v1"
GOLD_WINDOW_SECONDS = get_gold_window_seconds(60)
GOLD_WINDOW_DURATION = f"{GOLD_WINDOW_SECONDS} seconds"
GOLD_WATERMARK_SECONDS = get_gold_watermark_seconds(600)
GOLD_WATERMARK_DURATION = f"{GOLD_WATERMARK_SECONDS} seconds"
GOLD_TRIGGER_SECONDS = get_gold_trigger_seconds(5)
GOLD_SENTIMENT_ENDPOINT = get_gold_sentiment_endpoint("http://host.docker.internal:8000/predict")
GOLD_SENTIMENT_TIMEOUT_SECONDS = get_gold_sentiment_timeout_seconds(10)
GOLD_SENTIMENT_MAX_CONCURRENT_REQUESTS = get_gold_sentiment_max_concurrent_requests(4)
GOLD_SENTIMENT_CHUNK_SIZE = get_gold_sentiment_chunk_size(50)
APP_NAME = f"{get_spark_app_name()}-gold"
GOLD_REALTIME_CSV_PATH = str((Path(GOLD_DELTA_PATH).parent / "gold_sentiment_realtime.csv").resolve())
EMA_LOOKBACK = 10

# ── Canonical symbol order — drives pivot column ordering in the CSV ───────────
SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "XRP"]

# Wide-format CSV: one row per minute_time, one sentiment-index column per symbol.
# Schema: minute_time | BTC_sentiment_index | ETH_sentiment_index | … | total_message_count
CSV_HEADERS = (
    ["minute_time"]
    + [f"{sym}_sentiment_index" for sym in SYMBOLS]
    + ["total_message_count"]
)


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
    """Restore per-symbol EMA history from the existing wide-format CSV."""
    if not csv_path.exists():
        return

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            minute_text = (row.get("minute_time") or "").strip()
            if not minute_text:
                continue
            try:
                minute_time = datetime.fromisoformat(minute_text)
            except ValueError:
                continue

            for sym in SYMBOLS:
                col = f"{sym}_sentiment_index"
                sentiment_text = (row.get(col) or "").strip()
                if not sentiment_text:
                    continue
                try:
                    sentiment_value = float(sentiment_text)
                except ValueError:
                    continue

                state = _csv_state_by_symbol.setdefault(sym, CsvSymbolState())
                state.sentiment_history.append(sentiment_value)
                if state.last_minute is None or minute_time >= state.last_minute:
                    state.last_minute = minute_time


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
            F.col("window_end"),
            F.col("symbol"),
            F.col("sentiment_index").cast("double").alias("sentiment_index"),
            F.col("avg_confidence").cast("double").alias("avg_confidence"),
            F.col("message_count").cast("long").alias("message_count"),
        )
        .where(F.col("window_end").isNotNull() & F.col("symbol").isNotNull())
        .withColumn("minute_time", F.date_trunc("minute", F.col("window_end")))
        .groupBy("minute_time", "symbol")
        .agg(
            F.avg("sentiment_index").alias("sentiment_index"),
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
            "avg_confidence": _to_float(row["avg_confidence"], 0.0),
            "message_count": _to_int(row["message_count"], 0),
        }

    return dict(extracted)


def _build_realtime_csv_rows(
    actual_rows_by_symbol: dict[str, dict[datetime, dict[str, float | int]]],
) -> list[dict[str, str]]:
    """Build one wide CSV row per minute_time covering ALL symbols.

    Schema per emitted row
    ----------------------
    minute_time              : ISO-8601 wall-clock minute
    {SYM}_sentiment_index    : sentiment index for that symbol (EMA-filled when unobserved)
    total_message_count      : sum of message_count across all symbols for that minute
    """
    now_minute = datetime.now().replace(second=0, microsecond=0)

    # Collect every minute that appears in at least one symbol's observed data.
    all_minutes: set[datetime] = set()
    for sym_data in actual_rows_by_symbol.values():
        all_minutes.update(sym_data.keys())

    # Always include the current wall-clock minute so we keep emitting during
    # stream idle periods (uses EMA fill for every symbol).
    all_minutes.add(now_minute)

    rows_to_write: list[dict[str, str]] = []

    for minute_time in sorted(all_minutes):
        # Skip if this minute has already been emitted for every symbol.
        all_written = all(
            _csv_state_by_symbol.get(sym) is not None
            and _csv_state_by_symbol[sym].last_minute is not None
            and _csv_state_by_symbol[sym].last_minute >= minute_time
            for sym in SYMBOLS
        )
        if all_written:
            continue

        total_message_count = 0
        row: dict[str, str] = {"minute_time": minute_time.isoformat()}

        for sym in SYMBOLS:
            state = _csv_state_by_symbol.setdefault(sym, CsvSymbolState())
            observed = actual_rows_by_symbol.get(sym, {}).get(minute_time)

            # First-ever row for this symbol — seed from the latest available actual minute.
            if observed is None and state.last_minute is None:
                latest_by_sym = actual_rows_by_symbol.get(sym, {})
                if latest_by_sym:
                    latest_minute = max(latest_by_sym.keys())
                    observed = latest_by_sym.get(latest_minute)

            if observed is not None:
                sentiment_index = _to_float(observed["sentiment_index"], 0.0)
                avg_confidence = _to_float(observed["avg_confidence"], state.last_avg_confidence)
                message_count = _to_int(observed["message_count"], 0)
            else:
                # EMA fill — no real data for this (symbol, minute) pair.
                if not state.sentiment_history:
                    sentiment_index = 0.0
                else:
                    sentiment_index = _ema_from_recent(list(state.sentiment_history), EMA_LOOKBACK)
                avg_confidence = state.last_avg_confidence
                message_count = 0

            row[f"{sym}_sentiment_index"] = f"{sentiment_index:.6f}"
            total_message_count += message_count

            # Advance per-symbol EMA state.
            state.sentiment_history.append(sentiment_index)
            state.last_avg_confidence = avg_confidence
            state.last_minute = minute_time

        row["total_message_count"] = str(total_message_count)
        rows_to_write.append(row)

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
            "t.symbol = s.symbol AND t.window_start = s.window_start AND t.window_end = s.window_end",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _build_fallback_delta_df(batch_df, rows_to_write: list[dict[str, str]]):
    """Build Delta rows for minutes where ALL symbols were EMA-filled (no real data).

    A CSV row is "fully fallback" when total_message_count == 0.
    The Delta table itself stays long-format (one Delta row per symbol), so we
    expand each wide CSV row back into N symbol rows here.
    """
    fallback_rows: list[Row] = []
    for row in rows_to_write:
        if _to_int(row.get("total_message_count"), -1) != 0:
            continue  # at least one symbol had real data – already upserted above

        try:
            minute_start = datetime.fromisoformat(str(row["minute_time"]))
        except (TypeError, ValueError, KeyError):
            continue

        for sym in SYMBOLS:
            sentiment_index = _to_float(row.get(f"{sym}_sentiment_index"), 0.0)
            fallback_rows.append(
                Row(
                    window_start=minute_start,
                    window_end=minute_start + timedelta(minutes=1),
                    symbol=sym,
                    sentiment_index=sentiment_index,
                    avg_confidence=0.0,
                    message_count=0,
                    window_date=minute_start.date(),
                )
            )

    if not fallback_rows:
        return None

    # Build from local rows and cast to the streaming schema to avoid Python-side
    # type mismatches (e.g. float values for LongType fields).
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
    aggregated_batch = None

    if has_batch_data:
        batch_df.persist()
        try:
            client = CryptoBertClient(
                endpoint=GOLD_SENTIMENT_ENDPOINT,
                timeout_seconds=GOLD_SENTIMENT_TIMEOUT_SECONDS,
            )
            scored_batch = score_sentiment_dataframe(
                batch_df,
                client=client,
                max_concurrent_requests=GOLD_SENTIMENT_MAX_CONCURRENT_REQUESTS,
                chunk_size=GOLD_SENTIMENT_CHUNK_SIZE,
            )
            aggregated_raw = GoldSentimentTransformer.transform(
                scored_batch,
                window_duration=GOLD_WINDOW_DURATION,
                watermark_duration=GOLD_WATERMARK_DURATION,
            )
            aggregated_batch = (
                aggregated_raw
                .select(
                    F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "symbol",
                    "sentiment_index",
                    "avg_confidence",
                    F.col("message_count").cast("int").alias("message_count"),
                )
                .withColumn("window_date", F.to_date("window_end"))
            )
            _upsert_gold_delta(aggregated_batch)
            actual_rows_by_symbol = _extract_actual_rows_by_minute(aggregated_batch)
            observed_groups = sum(len(rows) for rows in actual_rows_by_symbol.values())
        finally:
            batch_df.unpersist()

    rows_to_write = _build_realtime_csv_rows(actual_rows_by_symbol)
    _append_rows_to_csv(csv_path, rows_to_write)

    if aggregated_batch is not None:
        fallback_delta_df = _build_fallback_delta_df(aggregated_batch, rows_to_write)
        _upsert_gold_delta(fallback_delta_df)
    if has_batch_data:
        gold_state_events = (
            aggregated_batch
            .select(
                F.lit("aggregate").alias("source"),
                F.col("symbol"),
                F.col("window_end").alias("event_time"),
            )
            .where(F.col("event_time").isNotNull() & F.col("symbol").isNotNull())
        )
        upsert_sentiment_state(gold_state_events, layer="gold")
    fallback_groups = sum(
        1 for row in rows_to_write if _to_int(row.get("total_message_count"), -1) == 0
    )

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
    logger.info("Real-time gold CSV output: %s", GOLD_REALTIME_CSV_PATH)

    query: StreamingQuery = (
        silver_stream.writeStream
        .queryName(f"{APP_NAME}-write")
        .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
        .trigger(processingTime=f"{GOLD_TRIGGER_SECONDS} seconds")
        .foreachBatch(process_gold_batch)
        .start()
    )
    await_query(query)


if __name__ == "__main__":
    run()
