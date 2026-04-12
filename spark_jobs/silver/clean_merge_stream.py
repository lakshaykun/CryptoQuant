from spark_jobs.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

from collections.abc import Iterator

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    coalesce,
    col,
    from_unixtime,
    from_json,
    length,
    lower,
    regexp_replace,
    trim,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    StringType,
    IntegerType,
    TimestampType,
)

from spark_jobs.common.job_runtime import (
    await_query,
    configure_job_logging,
    log_stream_schema,
)
from spark_jobs.common.sentiment_client import CryptoBertClient
from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_gold_sentiment_endpoint,
    get_gold_sentiment_timeout_seconds,
    get_spark_app_name,
    get_spark_master,
)

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
SILVER_CHECKPOINT_ROOT = get_checkpoint_path("silver", "checkpoints/silver")
SILVER_CHECKPOINT_PATH = f"{SILVER_CHECKPOINT_ROOT.rstrip('/')}/clean_merge_stream"
SILVER_SENTIMENT_ENDPOINT = get_gold_sentiment_endpoint("http://127.0.0.1:8000/predict")
SILVER_SENTIMENT_TIMEOUT_SECONDS = get_gold_sentiment_timeout_seconds(10)
APP_NAME = f"{get_spark_app_name()}-silver"

scored_schema = StructType([
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

bronze_schema = StructType([
    StructField("raw_json", StringType()),
    StructField("topic", StringType()),
    StructField("partition", StringType()),
    StructField("offset", StringType()),
    StructField("kafka_timestamp", TimestampType()),
])


_SENTIMENT_CLIENT: CryptoBertClient | None = None


def _get_sentiment_client() -> CryptoBertClient:
    global _SENTIMENT_CLIENT
    if _SENTIMENT_CLIENT is None:
        _SENTIMENT_CLIENT = CryptoBertClient(
            endpoint=SILVER_SENTIMENT_ENDPOINT,
            timeout_seconds=SILVER_SENTIMENT_TIMEOUT_SECONDS,
        )
    return _SENTIMENT_CLIENT


def infer_sentiment(text: str):
    return _get_sentiment_client().infer_sentiment(text)


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        return

    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def build_parsed_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    ensure_delta_table(spark, BRONZE_DELTA_PATH, bronze_schema)
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(BRONZE_DELTA_PATH)
    )

    return (
        bronze_stream
        .select(from_json(col("raw_json"), scored_schema).alias("data"))
        .select("data.*")
    )


def load_existing_silver_ids(spark: SparkSession) -> DataFrame:
    try:
        return (
            spark.read
            .format("delta")
            .load(SILVER_DELTA_PATH)
            .select(col("id"))
            .dropna(subset=["id"])
            .dropDuplicates(["id"])
        )
    except Exception:
        # Bootstrap case: silver table does not exist yet.
        return spark.createDataFrame([], StructType([StructField("id", StringType(), True)]))


def clean_stream(parsed_stream):
    # Normalize noisy social/media text into stable NLP-ready tokens.
    cleaned = (
        parsed_stream
        .filter(col("text").isNotNull())
        .withColumn("text", regexp_replace("text", r"<[^>]+>", " "))
        .withColumn("text", regexp_replace("text", r"&amp;", " & "))
        .withColumn("text", regexp_replace("text", r"&lt;", " < "))
        .withColumn("text", regexp_replace("text", r"&gt;", " > "))
        .withColumn("text", regexp_replace("text", r"&(quot|#34);", " \" "))
        .withColumn("text", regexp_replace("text", r"&#39;", " ' "))
        .withColumn("text", lower(col("text")))
        .withColumn("text", regexp_replace("text", r"https?://\S+|www\.\S+", " "))
        .withColumn("text", regexp_replace("text", r"@(\w+)", " "))
        .withColumn("text", regexp_replace("text", r"#(\w+)", "$1"))
        .withColumn("text", regexp_replace("text", r"[^a-z0-9$%+\-\s]", " "))
        .withColumn("text", regexp_replace("text", r"\s+", " "))
        .withColumn("text", trim(col("text")))
        .withColumn("source_norm", lower(trim(col("source"))))
        .withColumn(
            "text",
            when(
                col("source_norm") == "reddit",
                regexp_replace(
                    regexp_replace(col("text"), r"\b([ru])/\w+\b", " "),
                    r"\b(removed|deleted)\b",
                    " ",
                ),
            ).otherwise(col("text")),
        )
        .withColumn(
            "text",
            when(
                col("source_norm") == "youtube",
                regexp_replace(
                    regexp_replace(col("text"), r"\b\d{1,2}:\d{2}(?::\d{2})?\b", " "),
                    r"\[[^\]]+\]",
                    " ",
                ),
            ).otherwise(col("text")),
        )
        .withColumn(
            "text",
            when(
                col("source_norm") == "news",
                regexp_replace(
                    regexp_replace(col("text"), r"\b(source|via)\s*:\s*\S+", " "),
                    r"\b(read more|continue reading)\b.*$",
                    " ",
                ),
            ).otherwise(col("text")),
        )
        .withColumn("text", regexp_replace("text", r"\s+", " "))
        .withColumn("text", trim(col("text")))
        .withColumn(
            "event_time",
            coalesce(
                to_timestamp(col("timestamp")),
                to_timestamp(from_unixtime(col("timestamp").cast("double"))),
                to_timestamp(from_unixtime((col("timestamp").cast("double") / 1000.0))),
            ),
        )
        .drop("source_norm")
    )

    return (
        cleaned
        .filter(col("event_time").isNotNull())
        .filter(length(col("text")) > 5)
        .filter(~col("text").rlike("(?i)100x|free money|pump signal|join now"))
        .filter(~col("text").rlike("(?i)whatsapp group|telegram signal"))
    )


def _score_partition(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdf_iter:
        if pdf.empty:
            yield pd.DataFrame(columns=[field.name for field in scored_schema.fields])
            continue

        labels: list[str] = []
        confidences: list[float] = []
        base_scores: list[float] = []

        for text in pdf["text"].fillna("").astype(str):
            label, confidence, base_score = infer_sentiment(text)
            labels.append(label)
            confidences.append(float(confidence))
            base_scores.append(float(base_score))

        pdf = pdf.copy()
        pdf["label"] = labels
        pdf["confidence"] = confidences
        pdf["weighted_sentiment"] = (
            pd.Series(base_scores, dtype="float64")
            * pd.Series(confidences, dtype="float64")
            * pdf["engagement"].astype("float64")
        )

        yield pdf[
            [
                "id",
                "timestamp",
                "source",
                "text",
                "engagement",
                "symbol",
                "event_time",
                "label",
                "confidence",
                "weighted_sentiment",
            ]
        ]


def build_scored_stream(cleaned_stream: DataFrame) -> DataFrame:
    return cleaned_stream.mapInPandas(_score_partition, schema=scored_schema)


def filter_existing_ids(df: DataFrame, existing_ids: DataFrame) -> DataFrame:
    return df.join(existing_ids, on="id", how="left_anti")


def process_silver_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    existing_silver_ids = load_existing_silver_ids(spark)
    new_rows = filter_existing_ids(batch_df, existing_silver_ids)

    if new_rows.rdd.isEmpty():
        return

    scored_batch = build_scored_stream(new_rows)
    scored_batch.write.format("delta").mode("append").save(SILVER_DELTA_PATH)


def start_silver_query(validated_stream: DataFrame) -> StreamingQuery:
    return (
        validated_stream.writeStream
        .queryName(f"{APP_NAME}-write")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .foreachBatch(process_silver_batch)
        .start()
    )


def run() -> None:
    configure_job_logging()

    parsed_stream = build_parsed_stream()
    validated_stream = clean_stream(parsed_stream)
    log_stream_schema(validated_stream, "silver_validated_stream")

    query = start_silver_query(validated_stream)
    await_query(query)


if __name__ == "__main__":
    run()