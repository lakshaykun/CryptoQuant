from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel


class CryptoFeatures(BaseModel):
    open_time: datetime
    symbol: str = "BTCUSDT"

    # core (minimal required)
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    taker_buy_base: float

    # derived base
    hl_range: float
    vwap_proxy: float

    # returns
    return_current: float
    log_return_lag1: float
    return_5: float
    return_1d: float
    return_3d: float
    return_zscore: float
    return_acceleration: float
    smoothed_return_3: float

    # volatility
    volatility: float
    volatility_5: float
    volatility_std_10: float
    volatility_1d: float
    volatility_3d: float
    volatility_ratio: float
    volatility_regime: float

    # microstructure
    imbalance_ratio: float
    imbalance_change: float
    imbalance_momentum: float
    buy_ratio: float
    buy_pressure_change: float

    # price positioning
    price_to_ma_5: float
    price_to_ma_20: float
    ma_cross_5_20: float

    # momentum
    momentum_ratio: float

    # volume & activity
    volume_spike: float
    volume_ratio: float
    volume_trend: float
    trades_ratio: float

    # candle structure
    body_size: float
    close_position: float
    range_ratio: float

    # time encoding
    hour_sin: float
    hour_cos: float

    # optional (evaluation / backtesting only)
    actual_log_return_lead1: Optional[float] = None


class PredictRequest(BaseModel):
    data: List[CryptoFeatures]


class PredictResponse(BaseModel):
    predictions: Dict[str, List[float | int]]