from __future__ import annotations

from datetime import datetime, timedelta, timezone
import concurrent.futures
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.ingestion.streaming.sources.sentiment.shared.ingestion_state import parse_utc_timestamp
from pipelines.jobs.sentiment.config import (
    SUPPORTED_SENTIMENT_SOURCES,
    SentimentExecutionConfig,
    load_sentiment_pipeline_config,
)
from pipelines.jobs.streaming.common.sentiment_client import CryptoBertClient
from pipelines.jobs.streaming.common.sentiment_scoring import score_sentiment_dataframe
from pipelines.jobs.streaming.common.sentiment_state import (
    get_sentiment_state_path,
    upsert_sentiment_state,
)
from pipelines.schema.bronze.sentiment import BRONZE_SENTIMENT_SCHEMA
from pipelines.schema.gold.sentiment import GOLD_SENTIMENT_SCHEMA
from pipelines.schema.gold.processed_sentiment import GOLD_PROCESSED_SENTIMENT_SCHEMA
from pipelines.schema.silver.sentiment import SILVER_SENTIMENT_SCHEMA
from pipelines.storage.delta.utils import get_table_config
from pipelines.storage.delta.writer import write_batch
from pipelines.transformers.gold.sentiment import GoldSentimentTransformer
from pipelines.transformers.silver.sentiment import SilverSentimentTransformer
from utils.config_loader import load_config
from utils.logger import get_logger
from utils.spark_config import (
    get_gold_sentiment_chunk_size,
    get_gold_sentiment_endpoint,
    get_gold_sentiment_max_concurrent_requests,
    get_gold_sentiment_timeout_seconds,
    get_gold_watermark_seconds,
    get_gold_window_seconds,
)

logger = get_logger("sentiment_pipeline_core")

DATA_CONFIG = load_config("configs/data.yaml")
WINDOW_DURATION = f"{get_gold_window_seconds(60)} seconds"
WATERMARK_DURATION = f"{get_gold_watermark_seconds(600)} seconds"
SENTIMENT_ENDPOINT = get_gold_sentiment_endpoint("http://127.0.0.1:8000/predict")
SENTIMENT_TIMEOUT_SECONDS = get_gold_sentiment_timeout_seconds(10)
SENTIMENT_MAX_CONCURRENT_REQUESTS = get_gold_sentiment_max_concurrent_requests(4)
SENTIMENT_CHUNK_SIZE = get_gold_sentiment_chunk_size(50)

def _get_source_fetchers() -> dict[str, Callable[..., list[dict]]]:
    """Lazily import source fetchers so non-ingest stages don't require optional deps."""
    from pipelines.ingestion.streaming.sources.sentiment.news.source import fetch_news_events
    from pipelines.ingestion.streaming.sources.sentiment.reddit.source import fetch_reddit_events
    from pipelines.ingestion.streaming.sources.sentiment.telegram.source import fetch_telegram_events
    from pipelines.ingestion.streaming.sources.sentiment.youtube.source import fetch_youtube_events

    return {
        "reddit": fetch_reddit_events,
        "youtube": fetch_youtube_events,
        "news": fetch_news_events,
        "telegram": fetch_telegram_events,
    }


def _table_path(table_name: str) -> str:
    return get_table_config(table_name, DATA_CONFIG)["path"]


def _is_empty(df: DataFrame | None) -> bool:
    if df is None:
        return True
    return df.limit(1).count() == 0


def supported_ingest_sources(mode: str) -> list[str]:
    return load_sentiment_pipeline_config(mode).sources


def _latest_state_by_key(spark, layer: str) -> DataFrame | None:
    state_path = get_sentiment_state_path()
    if not DeltaTable.isDeltaTable(spark, state_path):
        return None

    state_df = spark.read.format("delta").load(state_path)
    if state_df.limit(1).count() == 0:
        return None

    return (
        state_df.where(F.col("layer") == layer)
        .groupBy("source", "symbol")
        .agg(F.max("last_processed_time").alias("last_processed_time"))
    )


def _filter_incremental(df: DataFrame, spark, layer: str) -> DataFrame:
    state_df = _latest_state_by_key(spark, layer)
    if state_df is None:
        return df

    return (
        df.alias("d")
        .join(state_df.alias("s"), on=["source", "symbol"], how="left")
        .where(F.col("s.last_processed_time").isNull() | (F.col("d.event_time") > F.col("s.last_processed_time")))
        .select("d.*")
    )


def _normalize_to_bronze(events_df: DataFrame) -> DataFrame:
    return (
        events_df.withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("engagement", F.col("engagement").cast("int"))
        .where(F.col("id").isNotNull() & F.col("text").isNotNull() & F.col("event_time").isNotNull())
        .dropDuplicates(["id", "source", "symbol", "event_time"])
    )


def _latest_processed_for_sources(spark, layer: str, sources: list[str]) -> datetime | None:
    state_df = _latest_state_by_key(spark, layer)
    if state_df is None:
        return None

    scoped = state_df.where(F.col("source").isin(sources))
    if scoped.limit(1).count() == 0:
        return None

    latest = scoped.select(F.max("last_processed_time").alias("ts")).collect()[0]["ts"]
    if latest is None:
        return None

    if latest.tzinfo is None:
        return latest.replace(tzinfo=timezone.utc)
    return latest.astimezone(timezone.utc)


def _resolve_batch_since_time(
    config: SentimentExecutionConfig,
    last_processed: datetime | None,
) -> datetime:
    now_utc = datetime.now(timezone.utc)
    fallback = now_utc - timedelta(minutes=config.fallback_lookback_minutes)
    start_time = config.start_time or fallback
    if last_processed is None:
        return start_time
    return max(start_time, last_processed)


def _planned_ingest_windows(
    spark,
    execution_mode: str,
    selected_sources: list[str],
    config: SentimentExecutionConfig,
) -> dict[str, dict[str, object]]:
    now_utc = datetime.now(timezone.utc)
    windows: dict[str, dict[str, object]] = {}

    for source in selected_sources:
        aliases = config.source_aliases.get(source, [source])

        if execution_mode == "streaming":
            since_time = now_utc - timedelta(minutes=config.lookback_minutes or config.fallback_lookback_minutes)
        else:
            last_processed = _latest_processed_for_sources(spark, config.bronze_state_layer, aliases)
            since_time = _resolve_batch_since_time(config, last_processed)

        delta_minutes = int(max(0, (now_utc - since_time).total_seconds() // 60))
        lookback_minutes = max(
            config.min_lookback_minutes,
            delta_minutes + config.query_safety_buffer_minutes,
        )

        windows[source] = {
            "since_time": since_time,
            "lookback_minutes": lookback_minutes,
            "aliases": aliases,
        }

    return windows


def _infer_source_key(event_source: str, reverse_aliases: dict[str, str]) -> str | None:
    normalized = str(event_source or "").strip().lower()
    if not normalized:
        return None
    return reverse_aliases.get(normalized)


def _filter_events_by_window(events: list[dict], windows: dict[str, dict[str, object]]) -> list[dict]:
    reverse_aliases: dict[str, str] = {}
    for source_key, plan in windows.items():
        for alias in plan.get("aliases", []):
            reverse_aliases[str(alias).strip().lower()] = source_key

    accepted: list[dict] = []
    for event in events:
        source_key = _infer_source_key(event.get("source", ""), reverse_aliases)
        if source_key is None:
            continue

        event_time = parse_utc_timestamp(str(event.get("timestamp") or ""))
        if event_time is None:
            continue

        since_time = windows[source_key]["since_time"]
        if event_time <= since_time:
            continue

        accepted.append(event)

    return accepted


def _fetch_events_for_windows(windows: dict[str, dict[str, object]]) -> list[dict]:
    all_events: list[dict] = []
    source_fetchers = _get_source_fetchers()

    def _fetch_one(source_key: str, plan: dict[str, object]) -> list[dict]:
        fetcher = source_fetchers.get(source_key)
        if fetcher is None:
            logger.warning("No fetcher registered for source=%s", source_key)
            return []

        fetched = fetcher(
            lookback_minutes=int(plan["lookback_minutes"]),
            emit_current_timestamp=False,
        )
        logger.info(
            "Fetched source=%s rows=%d lookback_minutes=%s since=%s",
            source_key,
            len(fetched),
            plan["lookback_minutes"],
            plan["since_time"],
        )
        return fetched

    max_workers = max(1, min(len(windows), 4))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_source = {
            pool.submit(_fetch_one, source_key, plan): source_key
            for source_key, plan in windows.items()
        }
        for future in concurrent.futures.as_completed(future_to_source):
            source_key = future_to_source[future]
            try:
                all_events.extend(future.result())
            except Exception:
                logger.exception("Sentiment fetch failed for source=%s", source_key)

    return _filter_events_by_window(all_events, windows)


def run_ingest(spark, execution_mode: str, source: str = "all") -> int:
    config = load_sentiment_pipeline_config(execution_mode)
    selected_sources = config.sources if source == "all" else [source]
    windows = _planned_ingest_windows(
        spark,
        execution_mode=execution_mode,
        selected_sources=selected_sources,
        config=config,
    )

    events = _fetch_events_for_windows(windows)
    for event in events:
        event["id"] = event.get("id") or ""
        event["timestamp"] = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        event["source"] = event.get("source") or "unknown"
        event["text"] = event.get("text") or ""
        event["symbol"] = event.get("symbol") or "UNKNOWN"
        event["engagement"] = int(event.get("engagement") or 1)

    if not events:
        logger.info("No sentiment events fetched for mode=%s", execution_mode)
        return 0

    raw_df = spark.createDataFrame(events, schema=BRONZE_SENTIMENT_SCHEMA)
    bronze_df = _normalize_to_bronze(raw_df)
    bronze_df = _filter_incremental(bronze_df, spark, layer=config.bronze_state_layer)
    bronze_df = bronze_df.cache()
    bronze_count = int(bronze_df.count())

    if bronze_count == 0:
        bronze_df.unpersist()
        logger.info("No new sentiment bronze rows after state filter for mode=%s", execution_mode)
        return 0

    write_batch(
        bronze_df,
        config.bronze_table,
        BRONZE_SENTIMENT_SCHEMA,
        upsert=False,
        merge_schema=True,
        optimize_partitions=True,
    )
    upsert_sentiment_state(bronze_df, layer=config.bronze_state_layer)
    bronze_df.unpersist()
    return bronze_count


def run_bronze(spark, execution_mode: str) -> int:
    config = load_sentiment_pipeline_config(execution_mode)
    bronze_path = _table_path(config.bronze_table)
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        logger.info("Bronze sentiment table missing at %s", bronze_path)
        return 0

    bronze_count = spark.read.format("delta").load(bronze_path).count()
    logger.info("Bronze stage ready mode=%s table=%s rows=%d", execution_mode, config.bronze_table, bronze_count)
    return int(bronze_count)


def run_silver(spark, execution_mode: str) -> int:
    config = load_sentiment_pipeline_config(execution_mode)
    bronze_path = _table_path(config.bronze_table)
    if not DeltaTable.isDeltaTable(spark, bronze_path):
        logger.info("Bronze sentiment table missing at %s", bronze_path)
        return 0

    bronze_df = spark.read.format("delta").load(bronze_path)
    bronze_df = _filter_incremental(bronze_df, spark, layer=config.silver_state_layer)

    if _is_empty(bronze_df):
        logger.info("No new sentiment rows for silver mode=%s", execution_mode)
        return 0

    staged_df = bronze_df.withColumn("kafka_timestamp", F.lit(None).cast("timestamp"))
    cleaned_df = SilverSentimentTransformer.transform(staged_df)
    if _is_empty(cleaned_df):
        logger.info("No rows after silver cleaning for mode=%s", execution_mode)
        return 0

    silver_df = (
        cleaned_df.withColumn("event_date", F.to_date("event_time"))
        .where(F.col("event_time").isNotNull() & F.col("event_date").isNotNull())
        .dropDuplicates(["id", "source", "symbol", "event_time"])
    )
    if _is_empty(silver_df):
        logger.info("No rows after silver governance checks for mode=%s", execution_mode)
        return 0

    write_batch(
        silver_df,
        config.silver_table,
        SILVER_SENTIMENT_SCHEMA,
        upsert=False,
        optimize_partitions=True,
    )
    upsert_sentiment_state(silver_df, layer=config.silver_state_layer)
    return silver_df.count()


def run_gold(spark, execution_mode: str, reprocess: bool = False) -> int:
    config = load_sentiment_pipeline_config(execution_mode)
    silver_path = _table_path(config.silver_table)
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.info("Silver sentiment table missing at %s", silver_path)
        return 0

    silver_df = spark.read.format("delta").load(silver_path)
    if reprocess:
        logger.info("Gold stage reprocess enabled; skipping incremental state filter for mode=%s", execution_mode)
    else:
        silver_df = _filter_incremental(silver_df, spark, layer=config.gold_state_layer)

    if _is_empty(silver_df):
        logger.info(
            "No new sentiment rows for gold mode=%s reprocess=%s layer=%s",
            execution_mode,
            reprocess,
            config.gold_state_layer,
        )
        return 0

    client = CryptoBertClient(endpoint=SENTIMENT_ENDPOINT, timeout_seconds=SENTIMENT_TIMEOUT_SECONDS)
    scored_df = score_sentiment_dataframe(
        silver_df,
        client=client,
        max_concurrent_requests=SENTIMENT_MAX_CONCURRENT_REQUESTS,
        chunk_size=SENTIMENT_CHUNK_SIZE,
    )
    if _is_empty(scored_df):
        logger.info("No scored sentiment rows for gold mode=%s", execution_mode)
        return 0

    window_duration = f"{config.gold_window_seconds} seconds"
    gold_df = GoldSentimentTransformer.transform(
        scored_df,
        window_duration=window_duration,
        watermark_duration=WATERMARK_DURATION,
    )
    if _is_empty(gold_df):
        logger.info("No gold aggregates produced for mode=%s", execution_mode)
        return 0

    flattened_gold = (
        gold_df.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.col("sentiment_index").cast("double").alias("sentiment_index"),
            F.col("avg_confidence").cast("double").alias("avg_confidence"),
            F.col("message_count").cast("int").alias("message_count"),
        ).withColumn("window_date", F.to_date("window_start"))
    )

    write_batch(
        flattened_gold,
        config.gold_table,
        GOLD_SENTIMENT_SCHEMA,
        upsert=False,
        optimize_partitions=True,
    )

    gold_state_rows = silver_df.select("source", "symbol", "event_time")
    upsert_sentiment_state(gold_state_rows, layer=config.gold_state_layer)
    return flattened_gold.count()


def run_gold_processed(spark, execution_mode: str) -> int:
    config = load_sentiment_pipeline_config(execution_mode)
    gold_path = _table_path(config.gold_table)
    processed_table = f"{config.gold_table}_processed"
    processed_layer = f"{config.gold_state_layer}_processed"

    if not DeltaTable.isDeltaTable(spark, gold_path):
        logger.info("Gold sentiment table missing at %s", gold_path)
        return 0

    gold_df = spark.read.format("delta").load(gold_path)
    if _is_empty(gold_df):
        logger.info("No rows in gold sentiment table for mode=%s", execution_mode)
        return 0

    step_expr = f"INTERVAL {int(config.gold_window_seconds)} SECONDS"
    bounds = gold_df.groupBy("symbol").agg(
        F.min("window_start").alias("min_window_start"),
        F.max("window_start").alias("max_window_start"),
    )
    continuous_windows = bounds.select(
        "symbol",
        F.explode(F.expr(f"sequence(min_window_start, max_window_start, {step_expr})")).alias("window_start_time"),
    )

    observed = (
        gold_df.withColumn("window_start_time", F.col("window_start"))
        .groupBy("symbol", "window_start_time")
        .agg(F.avg("sentiment_index").alias("aggregated_sentiment"))
    )

    filled = (
        continuous_windows.alias("w")
        .join(observed.alias("o"), on=["symbol", "window_start_time"], how="left")
        .withColumn("observed_sentiment", F.col("aggregated_sentiment"))
    )

    history_window = (
        Window.partitionBy("symbol")
        .orderBy("window_start_time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Weighted trailing average provides stable EMA-like fill without full-table UDF scans.
    ema_filled = (
        filled.withColumn(
            "history_sentiments",
            F.filter(F.collect_list("observed_sentiment").over(history_window), lambda x: x.isNotNull()),
        )
        .withColumn(
            "ema_from_history",
            F.expr(
                "aggregate(history_sentiments, named_struct('v', cast(0.0 as double), 'init', false), "
                "(acc, x) -> named_struct('v', CASE WHEN acc.init THEN (0.4 * x) + (0.6 * acc.v) ELSE x END, 'init', true), "
                "acc -> CASE WHEN acc.init THEN acc.v ELSE cast(null as double) END)"
            ),
        )
        .withColumn("aggregated_sentiment", F.coalesce(F.col("observed_sentiment"), F.col("ema_from_history"), F.lit(0.0)))
        .withColumn("ema_filled_flag", F.col("observed_sentiment").isNull())
        .withColumn("date", F.to_date("window_start_time"))
        .select("symbol", "window_start_time", "aggregated_sentiment", "ema_filled_flag", "date")
    )

    write_batch(
        ema_filled,
        processed_table,
        GOLD_PROCESSED_SENTIMENT_SCHEMA,
        upsert=False,
        optimize_partitions=True,
    )

    state_rows = (
        ema_filled.select(
            F.lit("gold_processed").alias("source"),
            "symbol",
            F.col("window_start_time").alias("event_time"),
        )
    )
    upsert_sentiment_state(state_rows, layer=processed_layer)
    return int(ema_filled.count())


def validate_source(source: str) -> str:
    normalized = str(source).strip().lower()
    if normalized != "all" and normalized not in SUPPORTED_SENTIMENT_SOURCES:
        raise ValueError(f"Unsupported source '{source}'")
    return normalized
