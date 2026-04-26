# pipelines/schema/gold/market.py

from pyspark.sql.types import *


from pyspark.sql.types import *

GOLD_MARKET_SCHEMA = StructType([
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
    StructField("ma_50", DoubleType(), True),

    # rolling stats
    StructField("volatility_5", DoubleType(), True),
    StructField("volume_5", DoubleType(), True),
    StructField("buy_ratio_5", DoubleType(), True),
    StructField("volatility_std_10", DoubleType(), True),

    # return features
    StructField("return_5", DoubleType(), True),
    StructField("return_20", DoubleType(), True),

    # derived signals
    StructField("momentum_ratio", DoubleType(), True),
    StructField("volume_spike", DoubleType(), True),
    StructField("body_size", DoubleType(), True),
    StructField("volatility_ratio", DoubleType(), True),

    # trend features
    StructField("trend_strength", DoubleType(), True),
    StructField("trend_long", DoubleType(), True),
    StructField("trend_regime", IntegerType(), True),

    # microstructure dynamics
    StructField("imbalance_change", DoubleType(), True),

    # interactions
    StructField("volatility_momentum", DoubleType(), True),

    # mean reversion
    StructField("price_deviation", DoubleType(), True),

    # time features (encoded)
    StructField("hour_sin", DoubleType(), True),
    StructField("hour_cos", DoubleType(), True),

    # metadata
    StructField("is_valid_feature_row", BooleanType(), True),
    StructField("date", DateType(), True),
    StructField("ingestion_time", TimestampType(), True),
])