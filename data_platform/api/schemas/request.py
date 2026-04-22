from datetime import datetime

from pydantic import BaseModel
from typing import List, Optional

class CryptoFeatures(BaseModel):
    open_time: datetime 
    symbol: str = "BTCUSDT"

    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    taker_buy_base: float

    log_return: float
    volatility: float

    imbalance_ratio: float
    buy_ratio: float

    log_return_lag1: float
    log_return_lag2: float
    buy_ratio_lag1: float

    ma_5: float
    ma_20: float

    volatility_5: float
    volume_5: float
    buy_ratio_5: float

    momentum: float
    volume_spike: float
    price_range_ratio: float
    body_size: float

    hour: int
    day_of_week: int

    trend_strength: float
    volatility_ratio: float
    actual_log_return_lead1: Optional[float] = None


class PredictRequest(BaseModel):
    data: List[CryptoFeatures]