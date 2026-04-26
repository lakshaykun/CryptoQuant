# pipelines/schema/bronze/market.py

from pyspark.sql.types import *

BRONZE_MARKET_SCHEMA = StructType([
    StructField("symbol", StringType(), False),

    StructField("open_time", TimestampType(), False),
    StructField("close_time", TimestampType(), False),

    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),

    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),

    StructField("trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),

    # metadata
    StructField("date", DateType(), False),  # for partitioning
    StructField("source", StringType(), False),  # batch / stream
    StructField("ingestion_time", TimestampType(), False),
])