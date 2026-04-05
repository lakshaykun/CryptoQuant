import os
import sys
import json
import shutil
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _delta_table_id(delta_path: str) -> str | None:
    log_dir = Path(delta_path) / "_delta_log"
    if not log_dir.exists():
        return None

    for log_file in sorted(log_dir.glob("*.json")):
        try:
            for line in log_file.read_text(encoding="utf-8").splitlines():
                payload = json.loads(line)
                metadata = payload.get("metaData")
                if isinstance(metadata, dict):
                    table_id = metadata.get("id")
                    if table_id:
                        return str(table_id)
        except Exception:
            continue

    return None


def _checkpoint_delta_table_id(checkpoint_path: str) -> str | None:
    metadata_file = Path(checkpoint_path) / "metadata"
    if not metadata_file.exists():
        return None

    try:
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    except Exception:
        return None

    table_id = metadata.get("id")
    return str(table_id) if table_id else None


def _reset_stale_checkpoint(source_path: str, checkpoint_path: str) -> None:
    current_source_id = _delta_table_id(source_path)
    current_checkpoint_id = _checkpoint_delta_table_id(checkpoint_path)

    if not current_checkpoint_id or not current_source_id:
        return

    if current_checkpoint_id != current_source_id:
        shutil.rmtree(checkpoint_path, ignore_errors=True)

from pyspark.sql.functions import (
    col,
    from_json,
    length,
    lower,
    regexp_replace,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
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

_reset_stale_checkpoint(BRONZE_DELTA_PATH, SILVER_CHECKPOINT_PATH)

spark = create_delta_spark_session(APP_NAME, master=get_spark_master())

schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("text", StringType()),
    StructField("engagement", IntegerType()),
    StructField("symbol", StringType())
])

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_DELTA_PATH)
)

parsed = (
    bronze_stream
    .select(from_json(col("raw_json"), schema).alias("data"))
    .select("data.*")
)

cleaned = (
    parsed
    .filter(col("text").isNotNull())
    .withColumn("text", lower(col("text")))
    .withColumn("text", regexp_replace("text", r"http\S+", ""))
    .withColumn("text", regexp_replace("text", r"[@#]\w+", ""))
    .withColumn("event_time", to_timestamp("timestamp"))
)

validated = (
    cleaned
    .filter(length(col("text")) > 20)
    .filter(~col("text").rlike("(?i)100x|free money|pump signal|join now"))
    .filter(~col("text").rlike("(?i)whatsapp group|telegram signal"))
)
print("Starting silver layer streaming query...")
print("Schema of cleaned stream:")
validated.printSchema()
# Streaming DataFrames do not support batch actions like show().
# Use a console sink query separately for debugging if needed.


query = (
    validated.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .start(SILVER_DELTA_PATH)
)


query.awaitTermination()