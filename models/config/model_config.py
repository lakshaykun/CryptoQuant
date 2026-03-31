BASE_FEATURES = [
    "log_return",
    "volatility",
    "imbalance_ratio",
    "buy_ratio",
    "vwap"
]

LAG_FEATURES = [
    "log_return_lag1",
    "log_return_lag2",
    "buy_ratio_lag1"
]

ROLLING_FEATURES = [
    "ma_5",
    "ma_20",
    "volatility_5",
    "volume_5",
    "buy_ratio_5"
]

MARKET_FEATURES = [
    "momentum",
    "volume_spike",
    "price_range_ratio",
    "body_size",
    "trend_strength",
    "volatility_ratio"
]

TIME_FEATURES = [
    "hour",
    "day_of_week"
]

FEATURE_COLUMNS = (
    BASE_FEATURES
    + LAG_FEATURES
    + ROLLING_FEATURES
    + MARKET_FEATURES
    + TIME_FEATURES
)

TARGET_COLUMN = "target"

MODEL_PARAMS = {
    "n_estimators": 100,
    "max_depth": 6,
    "learning_rate": 0.05
}

TEST_SPLIT_RATIO = 0.2
MODEL_NAME = "crypto_price_model"