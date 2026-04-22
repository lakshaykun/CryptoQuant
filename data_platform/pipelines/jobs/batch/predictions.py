from datetime import datetime

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from models.inference.realtime import RealtimePredictor
from pipelines.schema.predictions.log_return_lead1 import PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA
from pipelines.schema.state.predictions import PREDICTIONS_STATE_SCHEMA
from pipelines.storage.delta.reader import get_last_processed_time_symbols, read_incremental_symbols
from pipelines.storage.delta.writer import write_batch
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


PREDICTIONS_BATCH_SCHEMA = StructType(
    [field for field in PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA.fields if field.name != "ingestion_time"]
)

PREDICTIONS_BATCH_COLUMNS = [field.name for field in PREDICTIONS_BATCH_SCHEMA.fields]


def _predict_batches(pdf_iter):
    predictor = RealtimePredictor()

    for pdf in pdf_iter:
        if pdf.empty:
            continue

        batch = pdf.copy()
        batch["prediction"] = np.asarray(predictor.predict(batch), dtype=float)
        batch = batch.drop(columns=["ingestion_time"], errors="ignore")
        yield batch[PREDICTIONS_BATCH_COLUMNS]


def _cast_prediction_columns(df):
    return df.selectExpr(
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


def main():
    logger = get_logger("predictions_job")

    spark = SparkSession.builder.appName("predictions-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        symbols = config.get("symbols") or []
        state_date_value = config.get("state_date") or config.get("start_date")

        if not symbols:
            raise ValueError("data.yaml must define 'symbols' for predictions")
        if not state_date_value:
            raise ValueError("data.yaml must define either 'state_date' or 'start_date'")

        state_date = datetime.fromisoformat(state_date_value)

        last_processed_times = get_last_processed_time_symbols(
            spark,
            "predictions_log_return_lead1_state",
            symbols,
            state_date,
        )

        df = read_incremental_symbols(
            spark,
            "gold_market",
            last_values=last_processed_times,
        )

        if df.rdd.isEmpty():
            logger.info("No new gold data to predict, skipping write")
            return

        df = df.filter(F.col("is_valid_feature_row") == True)

        if df.rdd.isEmpty():
            logger.info("No valid feature rows to predict, skipping write")
            return

        predicted_df = _cast_prediction_columns(
            df.mapInPandas(_predict_batches, schema=PREDICTIONS_BATCH_SCHEMA)
        )

        if predicted_df is None or predicted_df.rdd.isEmpty():
            logger.info("Prediction batch was empty, skipping write")
            return

        write_batch(
            predicted_df,
            "predictions_log_return_lead1",
            PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA,
        )

        previous_state_df = spark.createDataFrame(
            [(symbol, last_time) for symbol, last_time in last_processed_times.items()],
            ["symbol", "last_processed_time"],
        )

        current_state_df = predicted_df.groupBy("symbol").agg(
            F.max("open_time").alias("last_processed_time")
        )

        state_df = (
            previous_state_df
            .unionByName(current_state_df)
            .groupBy("symbol")
            .agg(F.max("last_processed_time").alias("last_processed_time"))
        )

        write_batch(
            state_df,
            "predictions_log_return_lead1_state",
            PREDICTIONS_STATE_SCHEMA,
            mode="overwrite",
            upsert=False,
        )

        logger.info("Batch predictions complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()