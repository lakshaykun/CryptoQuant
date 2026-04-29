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

    # derived features
    StructField("hl_range", DoubleType()),     # (high - low)
    StructField("vwap_proxy", DoubleType()),   # (high+low+close)/3

    # returns (clean)
    StructField("return_current", DoubleType()),
    StructField("log_return_lag1", DoubleType()),
    StructField("return_5", DoubleType()),
    StructField("return_1d", DoubleType()),
    StructField("return_3d", DoubleType()),
    StructField("return_zscore", DoubleType()),
    StructField("return_acceleration", DoubleType()),
    StructField("smoothed_return_3", DoubleType()),

    # volatility
    StructField("volatility", DoubleType()),
    StructField("volatility_5", DoubleType()),
    StructField("volatility_std_10", DoubleType()),
    StructField("volatility_1d", DoubleType()),
    StructField("volatility_3d", DoubleType()),
    StructField("volatility_ratio", DoubleType()),
    StructField("volatility_regime", DoubleType()),

    # microstructure
    StructField("imbalance_ratio", DoubleType()),
    StructField("imbalance_change", DoubleType()),
    StructField("imbalance_momentum", DoubleType()),
    StructField("buy_ratio", DoubleType()),
    StructField("buy_pressure_change", DoubleType()),

    # price positioning
    StructField("price_to_ma_5", DoubleType()),
    StructField("price_to_ma_20", DoubleType()),
    StructField("ma_cross_5_20", DoubleType()),

    # momentum
    StructField("momentum_ratio", DoubleType()),

    # volume
    StructField("volume_spike", DoubleType()),
    StructField("volume_ratio", DoubleType()),
    StructField("volume_trend", DoubleType()),
    StructField("trades_ratio", DoubleType()),

    # candle structure
    StructField("body_size", DoubleType()),
    StructField("close_position", DoubleType()),
    StructField("range_ratio", DoubleType()),

    # time
    StructField("hour_sin", DoubleType()),
    StructField("hour_cos", DoubleType()),

    # metadata
    StructField("is_valid_feature_row", BooleanType(), True),
    StructField("date", DateType(), True),
    StructField("ingestion_time", TimestampType(), True),
])