from pyspark.sql.types import *

crypto_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("symbol", StringType(), True),
])