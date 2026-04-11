EXPECTED_COLUMNS = [
    'open_time',
    'close',
    'volume',
    'trades',
    'taker_buy_base',
    'symbol',
    'log_return',
    'volatility',
    'imbalance_ratio',
    'buy_ratio',
    'vwap',
    'log_return_lag1',
    'log_return_lag2',
    'buy_ratio_lag1',
    'ma_5',
    'ma_20',
    'volatility_5',
    'volume_5',
    'buy_ratio_5',
    'momentum',
    'volume_spike',
    'price_range_ratio',
    'body_size',
    'hour',
    'day_of_week',
    'trend_strength',
    'volatility_ratio',
    'target'
]

def validate_schema(df):
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")