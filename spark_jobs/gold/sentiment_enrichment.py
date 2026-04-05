import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import requests
from pyspark.sql.functions import avg, col, count, sum as spark_sum, udf, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_spark_app_name,
    get_spark_master,
)

SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
GOLD_DELTA_PATH = get_delta_path("gold", "delta/gold")
GOLD_CHECKPOINT_PATH = get_checkpoint_path("gold", "checkpoints/gold")
APP_NAME = f"{get_spark_app_name()}-gold"

spark = create_delta_spark_session(APP_NAME, master=get_spark_master())

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_DELTA_PATH)
)

def infer_sentiment(text: str):
    try:
        # Assuming your local API server is now running kk08/CryptoBERT
        response = requests.post(
            "http://127.0.0.1:8000/predict",
            json={"text": text},
            timeout=10
        )
        result = response.json()

        label = result["label"]
        print(f"Inferred sentiment: {label} with confidence {result['confidence']} for text: {text[:50]}...")
        confidence = float(result["confidence"])

        # ADJUSTED: kk08/CryptoBERT typically uses Positive/Neutral/Negative labels
        # We map these to numerical values for quantitative analysis
        label_map = {
            "LABEL_1": 1.0,
            "LABEL_0": -1.0,
            "UNCERTAIN": 0.0

        }
        
        base_score = label_map.get(label, 0.0)

        return (
            label,
            confidence,
            base_score
        )

    except Exception:
        # Graceful fallback for API timeouts or connection errors
        return ("UNCERTAIN", 0.0, 0.0)

# Define the schema for the UDF output
schema = StructType([
    StructField("label", StringType()),
    StructField("confidence", DoubleType()),
    StructField("base_score", DoubleType())
])

sentiment_udf = udf(infer_sentiment, schema)

# Processing the stream
scored_stream = (
    silver_stream
    .withColumn("sentiment_results", sentiment_udf(col("text")))
    .select(
        "*",
        col("sentiment_results.label").alias("label"),
        col("sentiment_results.confidence").alias("confidence"),
        # Calculation: (Base Direction) * (AI Confidence) * (Social Reach/Engagement)
        (
            col("sentiment_results.base_score") 
            * col("sentiment_results.confidence") 
            * col("engagement")
        ).alias("weighted_sentiment")
    )
    .drop("sentiment_results") # Cleanup the struct column
)

gold = (
    scored_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("symbol")
    )
    .agg(
        avg("weighted_sentiment").alias("sentiment_index"),
        spark_sum("engagement").alias("total_engagement"),
        avg("confidence").alias("avg_confidence"),
        count("*").alias("message_count")
    )
)

# Sink to Delta Lake Gold Table
query = (
    gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
    .start(GOLD_DELTA_PATH)
)

query.awaitTermination()