import time

from pipelines.utils.spark import get_spark
from utils_global.logger import get_logger
from utils_global.config_loader import load_config

from pipelines.ingestion.streaming.utils.helpers import parse_kafka_message
from pipelines.schema.raw.market import RAW_MARKET_SCHEMA

from pipelines.transformers.bronze.market import BronzeMarketTransformer
from pipelines.transformers.silver.market import SilverMarketTransformer
from pipelines.transformers.gold.market import GoldMarketTransformer

from pipelines.schema.bronze.market import BRONZE_MARKET_SCHEMA
from pipelines.schema.silver.market import SILVER_MARKET_SCHEMA
from pipelines.schema.gold.market import GOLD_MARKET_SCHEMA

from pipelines.storage.delta.writer import write_batch

from pipelines.storage.delta.market_strem_state import load_last_offsets, save_offsets
from pipelines.jobs.streaming.kafka_batch import get_kafka_batch


logger = get_logger("market_stream_job")

kafkaConfig = load_config("configs/kafka.yaml")

spark = get_spark(logger, need_kafka=True)


def run():
    start = time.perf_counter()

    # -----------------------------
    # 1. LOAD STATE
    # -----------------------------
    offsets = load_last_offsets(spark)

    # -----------------------------
    # 2. READ KAFKA
    # -----------------------------
    df, topic = get_kafka_batch(spark, kafkaConfig["topics"]["market_stream"], kafkaConfig["brokers"], offsets)

    if df.rdd.isEmpty():
        logger.info("[batch] No new data")
        return

    parsed = parse_kafka_message(df, RAW_MARKET_SCHEMA)

    # -----------------------------
    # 3. BRONZE
    # -----------------------------
    bronze = BronzeMarketTransformer().transform(parsed, "batch")

    write_batch(bronze, "bronze_market", BRONZE_MARKET_SCHEMA)

    # -----------------------------
    # 4. SILVER
    # -----------------------------
    silver = SilverMarketTransformer().transform(bronze)

    write_batch(silver, "silver_market", SILVER_MARKET_SCHEMA)

    # -----------------------------
    # 5. GOLD
    # -----------------------------
    gold = GoldMarketTransformer().process_gold_stream_batch(silver)

    write_batch(gold, "gold_market", GOLD_MARKET_SCHEMA)

    # -----------------------------
    # 6. SAVE STATE (CRITICAL)
    # -----------------------------
    save_offsets(df, topic)

    duration = time.perf_counter() - start

    logger.info("[batch] done in %.3fs", duration)


if __name__ == "__main__":
    run()