# pipelines/storage/delta/market_state.py

from pyspark.sql import functions as F
from datetime import datetime, timezone

from pipelines.schema.state.market_stream import STATE_MARKET_SCHEMA
from pipelines.storage.delta.writer import write_batch
from pipelines.storage.delta.reader import read_table
from utils_global.config_loader import load_config


dataConfig = load_config("configs/data.yaml")


def load_last_offsets(spark, table_name="market_stream_state"):
    try:
        df = read_table(spark, table_name)

        offsets = (
            df.groupBy("topic", "partition")
            .agg(F.max("offset").alias("offset"))
            .collect()
        )

        result = {}
        for row in offsets:
            topic = row["topic"]
            partition = row["partition"]
            offset = row["offset"]

            if topic not in result:
                result[topic] = {}

            result[topic][str(partition)] = offset + 1  # start from next

        return result

    except Exception:
        # first run
        return None


def save_offsets(df, topic):
    """
    Extract max offsets from Kafka DF and store in Delta
    """
    offsets_df = (
        df.select("partition", "offset")
        .groupBy("partition")
        .agg(F.max("offset").alias("offset"))
        .withColumn("topic", F.lit(topic))
        .withColumn("updated_at", F.lit(datetime.now(timezone.utc)))
        .select("topic", "partition", "offset", "updated_at")
    )

    write_batch(
        offsets_df,
        "market_stream_state",
        expected_schema=STATE_MARKET_SCHEMA,
        upsert=False
    )

