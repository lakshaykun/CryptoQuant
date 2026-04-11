from datetime import datetime

from pydantic import BaseModel
from typing import List


class CryptoFeatures(BaseModel):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime
    quote_volume: float
    symbol: str = "BTCUSDT"
    trades: int
    taker_buy_base: float
    taker_buy_quote: float
    ignore: float


class PredictRequest(BaseModel):
    data: List[CryptoFeatures]


class CryptoEngineeredFeatures(BaseModel):
    open_time: datetime
    close: float
    volume: float
    trades: int
    taker_buy_base: float
    symbol: str = "BTCUSDT"

    log_return: float
    volatility: float
    imbalance_ratio: float
    buy_ratio: float
    vwap: float

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


class PredictEngineeredRequest(BaseModel):
    data: List[CryptoEngineeredFeatures]