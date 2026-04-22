from pyspark.sql import functions as F

from models.inference.realtime import RealtimePredictor
from pipelines.schema.predictions.log_return_lead1 import PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA
from pipelines.storage.delta.writer import write_batch
from pipelines.utils.spark import get_spark
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


logger = get_logger("spark_predictions")
data_config = load_config("configs/data.yaml")
spark = get_spark(logger)
predictor = None


def _process_predictions(df, epoch_id):
    global predictor

    if df is None or df.rdd.isEmpty():
        return

    valid_rows = df.filter(F.col("is_valid_feature_row") == True)

    if valid_rows.rdd.isEmpty():
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