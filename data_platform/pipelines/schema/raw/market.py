# pipelines/schema/raw/market.py

from pyspark.sql.types import *

RAW_MARKET_SCHEMA = StructType([
    StructField("symbol", StringType(), False),

    StructField("open_time", LongType(), False),
    StructField("close_time", LongType(), False),

    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),

    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),

    StructField("trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True),
])