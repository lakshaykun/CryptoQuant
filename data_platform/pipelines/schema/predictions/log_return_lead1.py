from pyspark.sql.types import *


PREDICTIONS_LOG_RETURN_LEAD1_SCHEMA = StructType([
    StructField("open_time", TimestampType(), False),
    StructField("symbol", StringType(), False),

    # core market data
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("trades", IntegerType(), True),
    StructField("taker_buy_base", DoubleType(), True),

    # returns & volatility
    StructField("log_return", DoubleType(), True),
    StructField("volatility", DoubleType(), True),

    # microstructure
    StructField("imbalance_ratio", DoubleType(), True),
    StructField("buy_ratio", DoubleType(), True),

    # lag features
    StructField("log_return_lag1", DoubleType(), True),
    StructField("log_return_lag2", DoubleType(), True),
    StructField("buy_ratio_lag1", DoubleType(), True),

    # moving averages
    StructField("ma_5", DoubleType(), True),
    StructField("ma_20", DoubleType(), True),

    # rolling stats
    StructField("volatility_5", DoubleType(), True),
    StructField("volume_5", DoubleType(), True),
    StructField("buy_ratio_5", DoubleType(), True),

    # derived signals
    StructField("momentum", DoubleType(), True),
    StructField("volume_spike", DoubleType(), True),
    StructField("price_range_ratio", DoubleType(), True),
    StructField("body_size", DoubleType(), True),

    # time features
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),

    # trend features
    StructField("trend_strength", DoubleType(), True),
    StructField("volatility_ratio", DoubleType(), True),

    # metadata
    StructField("is_valid_feature_row", BooleanType(), True),
    StructField("date", DateType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("ingestion_time", TimestampType(), True)
])