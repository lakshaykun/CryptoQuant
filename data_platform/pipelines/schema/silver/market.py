# pipelines/schema/silver/market.py

from pyspark.sql.types import *

SILVER_MARKET_SCHEMA = StructType([
    StructField("symbol", StringType()),

    StructField("open_time", TimestampType()),

    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),

    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),

    StructField("trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),

    # metadata
    StructField("date", DateType()),  # for partitioning
    StructField("ingestion_time", TimestampType())
])