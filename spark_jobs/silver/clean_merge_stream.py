from spark_jobs.common.runtime_env import configure_pyspark_python

configure_pyspark_python()

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
    StructType,
    StructField,
    StringType,
    IntegerType
)

from spark_jobs.common.checkpoint import reset_stale_checkpoint
from spark_jobs.common.job_runtime import (
    await_query,
    configure_job_logging,
    log_stream_schema,
    start_delta_query,
)
from utils.spark_utils import create_delta_spark_session
from utils.spark_config import (
    get_checkpoint_path,
    get_delta_path,
    get_spark_app_name,
    get_spark_master,
)

BRONZE_DELTA_PATH = get_delta_path("bronze", "delta/bronze")
SILVER_DELTA_PATH = get_delta_path("silver", "delta/silver")
SILVER_CHECKPOINT_ROOT = get_checkpoint_path("silver", "checkpoints/silver")
SILVER_CHECKPOINT_PATH = f"{SILVER_CHECKPOINT_ROOT.rstrip('/')}/clean_merge_stream"
APP_NAME = f"{get_spark_app_name()}-silver"

schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType())
])


def build_parsed_stream():
    spark = create_delta_spark_session(APP_NAME, master=get_spark_master())
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(BRONZE_DELTA_PATH)
    )

    return (
        bronze_stream
        .select(from_json(col("raw_json"), schema).alias("data"))
        .select("data.*")
    )


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
        .filter(length(col("text")) > 20)
        .filter(~col("text").rlike("(?i)100x|free money|pump signal|join now"))
        .filter(~col("text").rlike("(?i)whatsapp group|telegram signal"))
    )


def run() -> None:
    configure_job_logging()
    reset_stale_checkpoint(BRONZE_DELTA_PATH, SILVER_CHECKPOINT_PATH)

    parsed_stream = build_parsed_stream()
    validated_stream = clean_stream(parsed_stream)
    log_stream_schema(validated_stream, "silver_validated_stream")

    query = start_delta_query(
        validated_stream,
        output_path=SILVER_DELTA_PATH,
        checkpoint_path=SILVER_CHECKPOINT_PATH,
        query_name=f"{APP_NAME}-write",
    )
    await_query(query)


if __name__ == "__main__":
    run()