from datetime import datetime

from pydantic import BaseModel
from typing import List, Optional

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CryptoFeatures(BaseModel):
    open_time: datetime
    symbol: str = "BTCUSDT"

    # core
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    taker_buy_base: float

    # returns & volatility
    log_return: float
    volatility: float

    # microstructure
    imbalance_ratio: float
    buy_ratio: float

    # lags
    log_return_lag1: float
    log_return_lag2: float
    buy_ratio_lag1: float

    # moving averages
    ma_5: float
    ma_20: float
    ma_50: float

    # rolling stats
    volatility_5: float
    volume_5: float
    buy_ratio_5: float
    volatility_std_10: float

    # return features
    return_5: float
    return_20: float

    # derived signals
    momentum_ratio: float
    volume_spike: float
    body_size: float
    volatility_ratio: float

    # trend
    trend_strength: float
    trend_long: float
    trend_regime: int

    # microstructure dynamics
    imbalance_change: float

    # interactions
    volatility_momentum: float

    # mean reversion
    price_deviation: float

    # time encoding
    hour_sin: float
    hour_cos: float

    # optional (for evaluation only)
    actual_log_return_lead1: Optional[float] = None


class PredictRequest(BaseModel):
    data: List[CryptoFeatures]