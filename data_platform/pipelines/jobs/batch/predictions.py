from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from models.inference.api_client import predict_with_api
from pipelines.schema.predictions.log_return_lead1 import PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA
from pipelines.schema.state.predictions import PREDICTIONS_STATE_SCHEMA
from pipelines.storage.delta.reader import get_last_processed_time_symbols, read_incremental_symbols
from pipelines.storage.delta.writer import write_batch
from utils_global.config_loader import load_config
from utils_global.logger import get_logger


PREDICTIONS_BATCH_SCHEMA = StructType(
    [field for field in PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA.fields if field.name not in ["ingestion_time", "prediction"]]
)

PREDICTIONS_BATCH_COLUMNS = [field.name for field in PREDICTIONS_BATCH_SCHEMA.fields]


def _predict_batches(pdf_iter):
    for pdf in pdf_iter:
        if pdf.empty:
            continue

        batch = pdf.copy()
        outputs = predict_with_api(batch)
        for key in ["return_short", "return_long", "sign_short", "sign_long"]:
            values = outputs.get(key)
            if isinstance(values, list):
                batch[key] = values
        yield batch[PREDICTIONS_BATCH_COLUMNS]


def _cast_prediction_columns(df):
    return df.selectExpr(
        "cast(open_time as timestamp) as open_time",
        "cast(symbol as string) as symbol",
        "cast(date as date) as date",
        "cast(return_short as double) as return_short",
        "cast(return_long as double) as return_long",
        "cast(sign_short as int) as sign_short",
        "cast(sign_long as int) as sign_long",
    )


def main(df=None):
    logger = get_logger("predictions_job")

    spark = SparkSession.builder.appName("predictions-job").getOrCreate()

    try:
        config = load_config("configs/data.yaml")
        modelConfig = load_config("configs/model.yaml")
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

        # drop nulls in feature columns
        df = df.dropna(subset=modelConfig.get("features_long", modelConfig.get("features_short", modelConfig.get("features", []))))

        if df.rdd.isEmpty():
            logger.info("No valid feature rows to predict, skipping write")
            return

        predicted_df = _cast_prediction_columns(
            df.mapInPandas(_predict_batches, schema=PREDICTIONS_BATCH_SCHEMA)
        )

        if predicted_df is None or predicted_df.rdd.isEmpty():
            logger.info("Prediction batch was empty, skipping write")
            return
        
        predicted_df = predicted_df.withColumn("prediction", F.col("return_short"))
        
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