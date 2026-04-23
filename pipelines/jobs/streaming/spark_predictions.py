import time
from datetime import datetime, timezone

from prometheus_client import Gauge
from pyspark.sql import functions as F

from models.inference.realtime import RealtimePredictor
from pipelines.schema.predictions.log_return_lead1 import PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA
from pipelines.storage.delta.writer import write_batch
from pipelines.utils.spark import get_spark
from utils_global.config_loader import load_config
from utils_global.logger import get_logger
from utils_global.prometheus import build_registry, metric_name, push_registry


logger = get_logger("spark_predictions")
data_config = load_config("configs/data.yaml")
spark = get_spark(logger)
predictor = None


def _normalize_open_time(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _estimate_streaming_lag_seconds(df):
    max_open_time = df.select(F.max("open_time").alias("max_open_time")).collect()[0]["max_open_time"]
    max_open_time = _normalize_open_time(max_open_time)
    if max_open_time is None:
        return 0.0

    lag = (datetime.now(timezone.utc) - max_open_time).total_seconds()
    return float(max(lag, 0.0))


def _push_streaming_metrics(records_processed, batch_processing_time, streaming_lag):
    registry = build_registry()

    records_metric = Gauge(
        metric_name("records_processed"),
        "Number of records processed in the latest Spark predictions micro-batch.",
        ["pipeline_job"],
        registry=registry,
    )
    batch_time_metric = Gauge(
        metric_name("batch_processing_time_seconds"),
        "Processing time in seconds for the latest Spark predictions micro-batch.",
        ["pipeline_job"],
        registry=registry,
    )
    lag_metric = Gauge(
        metric_name("streaming_lag_seconds"),
        "Lag in seconds between now and latest open_time scored by prediction stream.",
        ["pipeline_job"],
        registry=registry,
    )

    records_metric.labels(pipeline_job="spark_predictions").set(float(records_processed))
    batch_time_metric.labels(pipeline_job="spark_predictions").set(float(batch_processing_time))
    lag_metric.labels(pipeline_job="spark_predictions").set(float(streaming_lag))

    push_registry(registry, job_name="spark_predictions", grouping_key={"component": "spark_predictions"})


def _process_predictions(df, epoch_id):
    global predictor

    if df is None:
        return

    start_time = time.perf_counter()

    valid_rows = df.filter(F.col("is_valid_feature_row") == True)

    records_processed = valid_rows.count()

    if records_processed == 0:
        return

    pdf = valid_rows.toPandas()

    if pdf.empty:
        return

    if predictor is None:
        predictor = RealtimePredictor()

    pdf = pdf.copy()
    pdf["prediction"] = predictor.predict(pdf)

    result_df = valid_rows.sparkSession.createDataFrame(pdf).selectExpr(
        "cast(open_time as timestamp) as open_time",
        "cast(symbol as string) as symbol",
        "cast(open as double) as open",
        "cast(high as double) as high",
        "cast(low as double) as low",
        "cast(close as double) as close",
        "cast(volume as double) as volume",
        "cast(trades as int) as trades",
        "cast(taker_buy_base as double) as taker_buy_base",
        "cast(log_return as double) as log_return",
        "cast(volatility as double) as volatility",
        "cast(imbalance_ratio as double) as imbalance_ratio",
        "cast(buy_ratio as double) as buy_ratio",
        "cast(log_return_lag1 as double) as log_return_lag1",
        "cast(log_return_lag2 as double) as log_return_lag2",
        "cast(buy_ratio_lag1 as double) as buy_ratio_lag1",
        "cast(ma_5 as double) as ma_5",
        "cast(ma_20 as double) as ma_20",
        "cast(volatility_5 as double) as volatility_5",
        "cast(volume_5 as double) as volume_5",
        "cast(buy_ratio_5 as double) as buy_ratio_5",
        "cast(momentum as double) as momentum",
        "cast(volume_spike as double) as volume_spike",
        "cast(price_range_ratio as double) as price_range_ratio",
        "cast(body_size as double) as body_size",
        "cast(hour as int) as hour",
        "cast(day_of_week as int) as day_of_week",
        "cast(trend_strength as double) as trend_strength",
        "cast(volatility_ratio as double) as volatility_ratio",
        "cast(is_valid_feature_row as boolean) as is_valid_feature_row",
        "cast(date as date) as date",
        "cast(prediction as double) as prediction",
    )

    write_batch(
        result_df,
        "predictions_log_return_lead1",
        expected_schema=PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA,
    )

    duration = time.perf_counter() - start_time
    lag_seconds = _estimate_streaming_lag_seconds(valid_rows)
    _push_streaming_metrics(records_processed, duration, lag_seconds)
    logger.info(
        "[spark_predictions] epoch=%s records=%s duration=%.3fs lag=%.3fs",
        epoch_id,
        records_processed,
        duration,
        lag_seconds,
    )


gold_path = data_config["tables"]["gold_market"]["path"]
checkpoint_path = data_config["checkpoints"]["predictions_log_return_lead1"]

gold_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .load(gold_path)
)

query = (
    gold_stream.writeStream
    .foreachBatch(_process_predictions)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

spark.streams.awaitAnyTermination()