# pipelines/storage/delta/writer.py

from typing import Optional
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from utils_global.logger import get_logger
from utils_global.config_loader import load_config
from pipelines.schema.validation import validate_schema
from pipelines.storage.delta.utils import get_table_config, add_metadata, validate_partitions
from delta.tables import DeltaTable
logger = get_logger(__name__)

# Load once (DRY + performance)
CONFIG = load_config("configs/data.yaml")


# ---------------------------
# Batch Writer
# ---------------------------

def write_batch(
    df: DataFrame,
    table_name: str,
    expected_schema: StructType,
    mode: str = "append",
    merge_schema: bool = False,
    upsert: bool = True,
    merge_condition: str | None = None,
    optimize_partitions: bool = False,
):
    try:
        table_config = get_table_config(table_name, CONFIG)
        path = table_config["path"]

        # Add metadata
        df = add_metadata(df)

        # Reorder columns to match schema
        df = df.select([field.name for field in expected_schema.fields])

        # Validate schema
        if not validate_schema(df, expected_schema):
            raise ValueError(f"Schema mismatch for table '{table_name}'")

        partition_cols = table_config.get("partition_by")
        validate_partitions(df, partition_cols)
        if partition_cols and optimize_partitions:
            df = df.repartition(*[F.col(partition) for partition in partition_cols])

        # ---------------------------
        # UPSERT LOGIC
        # ---------------------------
        if upsert and DeltaTable.isDeltaTable(df.sparkSession, path):
            delta_table = DeltaTable.forPath(df.sparkSession, path)
            condition = merge_condition or "t.symbol = s.symbol AND t.open_time = s.open_time"

            (
                delta_table.alias("t")
                .merge(
                    df.alias("s"),
                    condition,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            logger.info(f"[{table_name}] Merge successful → {path}")

        else:
            writer = df.write.format("delta").mode(mode)

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            if merge_schema:
                writer = writer.option("mergeSchema", "true")
            last_error = None
            for attempt in range(1, 4):
                try:
                    writer.save(path)
                    last_error = None
                    break
                except Exception as write_error:
                    last_error = write_error
                    if attempt == 3:
                        raise
                    time.sleep(0.5 * attempt)
            if last_error is not None:
                raise last_error

            logger.info(f"[{table_name}] Initial write successful → {path}")

        logger.info(f"[{table_name}] Batch write completed -> {df.count()} rows")
        df.show(5, truncate=False)
    except Exception as e:
        logger.error(f"[{table_name}] Delta write failed → {e}")
        raise


# ---------------------------
# Streaming Writer (Bronze only)
# ---------------------------

def write_stream(
    df: DataFrame,
    table_name: str,
    output_mode: str = "append",
    trigger: str = "10 seconds",
    query_name: Optional[str] = None
):
    """
    Writes streaming DataFrame to Delta Lake.
    Intended for ingestion (bronze layer).
    """

    try:
        table_config = get_table_config(table_name, CONFIG)

        # Add metadata
        df = add_metadata(df)

        # Validate partitions
        partition_cols = table_config.get("partition_by")
        validate_partitions(df, partition_cols)

        # Validate output mode
        if output_mode not in {"append", "complete", "update"}:
            raise ValueError(f"Invalid output_mode '{output_mode}'")

        # Build streaming writer
        writer = (
            df.writeStream
            .format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", table_config["checkpoint"])
            .trigger(processingTime=trigger)
        )

        if query_name:
            writer = writer.queryName(query_name)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        query = writer.start(table_config["path"])

        logger.info(f"[{table_name}] Streaming started → {table_config['path']}")

        return query

    except Exception as e:
        logger.error(f"[{table_name}] Streaming write failed → {e}")
        raise
